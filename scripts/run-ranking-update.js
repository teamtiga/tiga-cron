const pg = require('pg');
const { Pool } = pg;
const { MongoClient } = require('mongodb');
const { BedrockRuntimeClient, InvokeModelCommand } = require('@aws-sdk/client-bedrock-runtime');
const { OpenAI } = require("openai");
const fs = require('fs').promises;
const path = require('path');

// Instance of Postgres Interface
const pool = new Pool({
  connectionString: process.env.POSTGRES_URL ,
  options: '-c timezone=Asia/Kolkata' // Ensures query results are in IST
})

const client = new MongoClient(process.env.MONGODB_URI);
let db;

async function connectMongo() {
    if (!db) {
        try {
            await client.connect();
            db = client.db('tiga_logs');
            console.log('Connected to MongoDB');
        } catch (error) {
            console.error('MongoDB connection error:', error);
            throw error;
        }
    }
    return db;
}

/**
 * Logs messages with a specified level and context.
 * @param {string} level - Log level (e.g., 'info', 'warn', 'error').
 * @param {string} message - Log message.
 * @param {object} context - Additional context for the log.
 */
async function log(level, message, context = {}, adminLog = false) {
    const logEntry = {
        level,
        adminLog,
        message,
        ...context,
        timestamp: new Date(),
        _schemaVersion: 1,
        source: 'github-actions'
    };

    try {
        const db = await connectMongo();
        await db.collection('logs').insertOne(logEntry);
    } catch (error) {
        console.error('Logger Error', error.message);
    }
}

async function updateDriverRanking() {
    const requestDetails = {
        url: '/updateDriverRanking' // Mimics the API call logs
    };

    await log('info', 'API called', requestDetails, /*adminLog:*/ true);

    try {
        // Step 1: Get the revevant data from the database
        const rankEffectsQuery = `
            WITH ranked_windows AS (
                SELECT 
                    enforced_at AS start_time,
                    LEAD(enforced_at, 1, NOW()) OVER (ORDER BY enforced_at) AS end_time
                FROM driver_ranking
                GROUP BY enforced_at
            ),
            window_rankings AS (
                SELECT
                    w.start_time,
                    w.end_time,
                    dr.driver_id,
                    dr.rank,
                    COALESCE(dr.tag, '') AS tag,
                    d.last_updated_at
                FROM ranked_windows w
                JOIN driver_ranking dr ON w.start_time = dr.enforced_at
                JOIN driver d ON dr.driver_id = d.id AND NOT d.removed
            ),
            review_aggregates AS (
                SELECT
                    w.start_time,
                    w.end_time,
                    r.driver_id,
                    ROUND(AVG(r.stars), 2) AS avg_stars,
                    COUNT(*) AS rating_count,
                    ARRAY_AGG(r.comment) AS comments
                FROM review r
                JOIN ranked_windows w 
                    ON r.last_updated_at > w.start_time 
                    AND r.last_updated_at <= w.end_time
                GROUP BY w.start_time, w.end_time, r.driver_id
            )
            SELECT
            wr.start_time,
            wr.end_time,
            JSONB_AGG(
                JSONB_BUILD_OBJECT(
                    'rank', wr.rank,
                    'tag', wr.tag,
                    'driver_id', wr.driver_id,
                    'avg_stars', COALESCE(ra.avg_stars, 0),
                    'rating_count', COALESCE(ra.rating_count, 0),
                    'comments', COALESCE(ra.comments, null)
                ) ORDER BY wr.rank
            ) AS rank_and_effect
            FROM window_rankings wr
            LEFT JOIN review_aggregates ra 
            ON wr.driver_id = ra.driver_id 
            AND wr.start_time = ra.start_time 
            AND wr.end_time = ra.end_time
            GROUP BY wr.start_time, wr.end_time
            ORDER BY wr.start_time;
        `

        let { rows: rankEffects } = await pool.query(rankEffectsQuery);

        const activeDriversQuery = `
            SELECT id, last_updated_at
            FROM driver
            WHERE removed = FALSE
        `

        let { rows: activeDrivers } = await pool.query(activeDriversQuery);

        // Step 2. Enrich data with logs
        const db = await connectMongo();
        const logsCollection = db.collection('logs');

        rankEffects = await Promise.all(rankEffects.map(async (rankEffect) => {
            const drivers = rankEffect.rank_and_effect;
            const driverIds = drivers.map(d => d.driver_id.toString());

            const driverProfileViewsQuery = [
                {
                    $match: {
                        'message': 'API called',
                        'url': '/getAllContactsOf',
                        'timestamp': { 
                            $gt: new Date(rankEffect.start_time),
                            $lte: new Date(rankEffect.end_time)
                        },
                        'body.driver_id': { $in: driverIds.map(id => parseInt(id)) }
                    }
                },
                {
                    $group: {
                        _id: '$body.driver_id',
                        profile_views: { $sum: 1 },
                        distinct_users: { $addToSet: '$user_id' }
                    }
                },
                {
                    $project: {
                        profile_views: 1,
                        distinct_profile_views: { $size: '$distinct_users' }
                    }
                }
            ];

            let driverProfileViews = await logsCollection.aggregate(driverProfileViewsQuery).toArray();
           
            const profileViewsMap = {};
            driverProfileViews.forEach(result => {
                const driverId = parseInt(result._id, 10);
                profileViewsMap[driverId] = {
                    profile_views: result.profile_views,
                    distinct_profile_views: result.distinct_profile_views
                };
            });

            const enrichedDrivers = drivers.map(driver => ({
                ...driver,
                profile_views: profileViewsMap[driver.driver_id]?.profile_views || 0,
                distinct_profile_views: profileViewsMap[driver.driver_id]?.distinct_profile_views || 0
            }));

            return { ...rankEffect, rank_and_effect: enrichedDrivers };
        }));

        // Step 3. Insert the data to the prompt and make LLM call
        let SYSTEM_PROMPT = await fs.readFile(
            path.join(__dirname, 'prompts/rankingUpdatePrompt.md'), 
            'utf-8'
        );
        SYSTEM_PROMPT = SYSTEM_PROMPT.replace('{active_driver_ids}', JSON.stringify(activeDrivers));

        const prompt = JSON.stringify(rankEffects);
        const { ai_call_successful, new_rankings } = await makeLLMCall(SYSTEM_PROMPT, prompt, activeDrivers, requestDetails);

        if(!ai_call_successful){
            throw new Error('Failed to make LLM call');
        }

        // Step 4. Update the driver ranking in the database
        const ranking_timestamp = new Date();
        const ranking_update_query = `
            INSERT INTO driver_ranking (driver_id, rank, tag, enforced_at)
            SELECT 
                CAST(driver_id AS INTEGER),
                CAST(rank AS INTEGER),
                tag,
                $4
            FROM UNNEST($1::text[], $2::text[], $3::text[]) 
            AS new_ranking(driver_id, rank, tag)
        `;

        await pool.query(
            ranking_update_query,
            [
                new_rankings.map(r => r.driver_id),
                new_rankings.map(r => r.rank),
                new_rankings.map(r => r.tag),
                ranking_timestamp
            ]
        );

        await log('info', 'Successful response', { new_rankings, rankEffects, activeDrivers, ...requestDetails }, /*adminLog:*/ true);

        return { status: 'success' };
    } catch (error) {
        console.error('Error in updateDriverRanking:', error);
        await log('error', 'Error in updateDriverRanking', { error: error.message, stack: error.stack, ...requestDetails }, /*adminLog:*/ true);
        throw error;
    }
}

async function makeLLMCall(SYSTEM_PROMPT, prompt, activeDrivers, requestDetails, retries = 3) {
    let ai_response;

    while (retries > 0) {
        try {
            // Step 1: Call the LLM
            if(process.env.AI_PROVIDER === "aws"){
                const client = new BedrockRuntimeClient({
                    region: process.env.AWS_REGION_NAME,
                    credentials: {
                        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
                        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
                    }
                });

                const payload = {
                    anthropic_version: process.env.ANTHROPIC_VERSION,
                    system: SYSTEM_PROMPT,
                    messages: [
                        { role: 'user', content: [{ type: 'text', text: prompt }] },
                        { role: 'assistant', content: [{ type: 'text', text: 'Here is directly the JSON requested for with no additional text:\n{' }] }
                    ],
                    max_tokens: 3000,
                    temperature: 0.6
                };

                const command = new InvokeModelCommand({
                    modelId: process.env.ANTHROPIC_MODEL_ID,
                    accept: 'application/json',
                    contentType: 'application/json',
                    body: JSON.stringify(payload)
                });
                
                const response = await client.send(command);
                const textContent = JSON.parse(Buffer.from(response.body).toString()).content[0].text;
                console.log('text content: ', textContent);
                ai_response = JSON.parse('{' + textContent.slice(0, textContent.lastIndexOf("}") + 1));
            } else if (process.env.AI_PROVIDER === "azure") {
                const client = new OpenAI({
                    apiKey: process.env.AZURE_API_KEY,
                    baseURL: `${process.env.AZURE_ENDPOINT}/openai/deployments/${process.env.AZURE_MODEL_ID}`,
                    defaultQuery: {
                        "api-version": process.env.AZURE_API_VERSION
                    },
                    defaultHeaders: {
                        "api-key": process.env.AZURE_API_KEY,
                    },
                });

                const messages = [
                    { role: 'system', content: SYSTEM_PROMPT },
                    { role: 'user', content: prompt }
                ];
                
                const response = await client.chat.completions.create({
                    messages,
                    temperature: 0.6,
                    max_tokens: 3000,
                    response_format: { type: 'json_object' }
                });

                ai_response = JSON.parse(response.choices[0].message.content);
            } else {
                throw new Error('Invalid provider - must be "aws" or "azure"');
            }

            // Step 2: Validate the ai_response
            const REQUIRED_FIELDS = ['driver_id', 'rank', 'tag'];
            if (!ai_response.ranking_results) throw new Error(`Invalid LLM output (${process.env.AI_PROVIDER}) - missing ranking_results key`);    
            if (Object.keys(ai_response).length !== 1) throw new Error(`Invalid LLM output (${process.env.AI_PROVIDER}) - additional fields present in ai_response`);
            if(!Array.isArray(ai_response.ranking_results)) throw new Error(`Invalid LLM output (${process.env.AI_PROVIDER}) - ranking_results is NOT an array.`)

            const activeDriverIds = new Set(activeDrivers.map(d => d.id));
            const seenRanks = new Set();
            const seenDriverIds = new Set();
            
            for (const result of ai_response.ranking_results) {
                if (REQUIRED_FIELDS.some(field => result[field] == null)) {
                    throw new Error(`Invalid LLM output (${process.env.AI_PROVIDER}) - missing required fields in ranking_results`);
                }
                if (Object.keys(result).length !== REQUIRED_FIELDS.length) {
                    throw new Error(`Invalid LLM output (${process.env.AI_PROVIDER}) - additional fields present in ranking_results`);
                }

                const driverId = Number(result.driver_id);
                const rank = Number(result.rank);

                if (!Number.isInteger(driverId)) throw new Error(`Invalid LLM output (${process.env.AI_PROVIDER}) - invalid driver_id: ${result.driver_id}`);
                if (!Number.isInteger(rank)) throw new Error(`Invalid LLM output (${process.env.AI_PROVIDER}) - invalid rank: ${result.rank}`);
                if (seenRanks.has(rank)) throw new Error(`Invalid LLM output (${process.env.AI_PROVIDER}) - duplicate rank: ${rank}`);
                if (seenDriverIds.has(driverId)) throw new Error(`Invalid LLM output (${process.env.AI_PROVIDER}) - duplicate driver_id: ${driverId}`);
                if (!activeDriverIds.has(driverId)) throw new Error(`Invalid LLM output (${process.env.AI_PROVIDER}) - driver ${driverId} NOT in active drivers`);

                seenRanks.add(rank);
                seenDriverIds.add(driverId);
            }

            if(seenDriverIds.size !== activeDriverIds.size) throw new Error(`Invalid LLM output (${process.env.AI_PROVIDER}) - didn't rank all active drivers`)
            
            return { ai_call_successful: true, new_rankings: ai_response.ranking_results };
        } catch (error) {
            retries--;
            console.error('Error in makeLLMCall:', error);
            await log('error', 'Error in makeLLMCall', { error: error.message, stack: error.stack,retries_left: retries, ...requestDetails }, /*adminLog*/ true);
        }
    }
    return { ai_call_successful: false, new_rankings: null };
}

// Execute the main function
(async () => {
    try {
        await updateDriverRanking();
        process.exit(0);
    } catch (error) {
        console.error('Script failed:', error);
        process.exit(1);
    } finally {
        // Close database connections
        await pool.end();
        await client.close();
    }
})();