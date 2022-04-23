import 'dotenv/config';
import got from 'got-cjs'
import { parseJSON } from 'date-fns'
import { Client, SSLMode, SSL } from 'ts-postgres'

const HOST = process.env.HOST
const PORT = process.env.PORT
const USER = process.env.USER
const PASSWORD = process.env.PASSWORD
const DB = process.env.DB


interface priceData {
    symbol?: string
    floorPrice?: number
    listedCount?: number
    avgPrice24hr?: number
    volumeAll?: number
}

interface twitterData {
    following?: boolean
    id: string
    screen_name: string
    name: string
    protected: boolean
    followers_count: number
    formatted_followers_count: string
    age_gated: boolean
}

const config = {
    launch: false,
    allNfts: false,
    prices: false,
    discord: true,
    twitter: false
}

let merged = []

const shallowEqual = async(object1, object2) => {
    const keys1 = Object.keys(object1);
    const keys2 = Object.keys(object2);
    if (keys1.length !== keys2.length) {
      return false;
    }
    for (let key of keys1) {
      if (object1[key] !== object2[key]) {
        return false;
      }
    }
    return true;
}

const timer = ms => new Promise(res => setTimeout(res, ms))

// TODO: Fetch once
const getMeNft = async(offset=0, limit=200, fData=[]) => {
    // TODO: Add support for paginating
    /*
    [
        {
		"symbol": "bc_ninjas",
		"name": "Blue Chips Ninjas",
		"description": "Blue Chips Ninjas is a collaborative space where members can share their best alpha info and help the rest of the community make good decisions when entering a project. \n\nHolders will be rewarded with $NINJAS tokens that can be used to access channels with more information, as well as to open their own alpha channel and inform the rest of Ninjas.",
		"image": "https://creator-hub-prod.s3.us-east-2.amazonaws.com/bc_ninjas_pfp_1649003523683.jpeg",
		"twitter": "https://www.twitter.com/BC_Ninjas",
		"discord": "https://www.discord.gg/bcninjas",
		"categories": [
                "art",
                "pfps"
            ]
        },
    ]
    */
    const url = `https://api-mainnet.magiceden.dev/v2/collections?offset=${offset}&limit=${limit}`
    try {
        const data = await got.get(url).json() as unknown as Array<object>
        
        merged = fData.concat(data)
        //console.log(merged)
        if (Object.keys(data).length >= limit) {
            // TODO: Issue with this if we have exactly 200 on last page...
            if(shallowEqual(fData, data)) {
                console.log('looping')
                
                // Recursion
                offset = offset + limit
                console.log(offset)
                await getMeNft(offset, limit, merged)
            }
        }
        //console.log(merged)
        // ToDo: Loop if length of data until data isn't 200 or 0
        //console.log(data)
        return merged
    } catch (e) {
        console.error(e)
    }
}

const getMeLaunchpadStats = async(offset=0, limit=200) => {
    /*
    [
        {
            "symbol": "hero_nft",
            "name": "HERO NFT",
            "description": "A culmination of dreams migrated onto the blockchain and stitched back into the fabric of reality. Collection of 10,101 unique Heroes.",
            "featured": false,
            "image": "https://dl.airtable.com/.attachmentThumbnails/4a86bad06744e1b2c7242bf1061595d5/795c2443",
            "price": 1,
            "size": 1000,
            "launchDatetime": "2021-11-24T07:00:00.000Z"
        },
    ]
    */
    const url = `https://api-mainnet.magiceden.dev/v2/launchpad/collections?offset=${offset}&limit=${limit}`
    const data = await got.get(url).json()
    // ToDo: Loop if length of data until data isn't 200 or 0
    //console.log(data)
    return data as object
}

// TODO: Fetch periodically
const getDiscordStats = async(inviteCode: string): Promise<object> => {
    /*
    {
        "code": "bcninjas",
        "type": 0,
        "expires_at": null,
        "guild": {
            "id": "943094133235523674",
            "name": "Blue Chips Ninjas",
            "splash": "902fb3f59216fc9716577124243ec046",
            "banner": "3fae6d6315f41cd03f334f41103f0ba0",
            "description": null,
            "icon": "902fb3f59216fc9716577124243ec046",
            "features": [
                "THREE_DAY_THREAD_ARCHIVE",
                "SEVEN_DAY_THREAD_ARCHIVE",
                "ROLE_ICONS",
                "MEMBER_PROFILES",
                "COMMUNITY",
                "INVITE_SPLASH",
                "ANIMATED_BANNER",
                "BANNER",
                "NEWS",
                "VANITY_URL",
                "ANIMATED_ICON",
                "PRIVATE_THREADS"
            ],
            "verification_level": 1,
            "vanity_url_code": "bcninjas",
            "premium_subscription_count": 26,
            "nsfw": false,
            "nsfw_level": 0
        },
        "channel": {
            "id": "946344494423482378",
            "name": "💼｜rules",
            "type": 0
        },
        "approximate_member_count": 5953,
        "approximate_presence_count": 2216
    }
    */
    const url = `https://discord.com/api/v8/invites/${inviteCode}?with_counts=true`
    const data = await got.get(url).json()
    //console.log(data)
    return data as object
}

const getMeCollectionStats = async(symbol: string) => {
    /*
    {
        "symbol": "bc_ninjas",
        "floorPrice": 790000000,
        "listedCount": 114,
        "avgPrice24hr": 1030487179.4871795,
        "volumeAll": 40189000000
    }
    */

    const url = `https://api-mainnet.magiceden.dev/v2/collections/${symbol}/stats`
    const data = await got.get(url).json()
    return data as priceData
}

const getTwitterStats = async(twitterName: string) => {
    /*
    [
        {
            "following": false,
            "id": "1473384507495620609",
            "screen_name": "BC_Ninjas",
            "name": "Solana Blue Chips Ninjas | MINTING TODAY 18:30",
            "protected": false,
            "followers_count": 2583,
            "formatted_followers_count": "2,583 followers",
            "age_gated": false
        }
    ]
    */
    const url = `https://cdn.syndication.twimg.com/widgets/followbutton/info.json?screen_names=${twitterName}`
    const data = await got.get(url).json()
    return data
}

const main = async() => {
    const client = new Client({
        host: HOST,
        user: USER,
        password: PASSWORD,
        database: DB,
        ssl: SSLMode.Disable, //SSLMode.Require as SSL,
        keepAlive: true
    })
    try {
        await client.connect()
        console.log("connected")
    } catch(e) {
        console.error(e)
    }
    
    if (config.launch) {
        let launchPad = await getMeLaunchpadStats()

        for(let pad in launchPad) {
            const symbol = launchPad[pad].symbol
            const name = launchPad[pad].name
            const description = launchPad[pad].description
            const image = launchPad[pad].image
            const price = launchPad[pad].price
            const size = launchPad[pad].size
            let launchDate = launchPad[pad].launchDatetime
            if (launchDate) {
                launchDate = parseJSON(launchDate)
                console.log(launchDate)
            } else {
                launchDate = null
            }

            try {
                const result = await client.query(
                    `SELECT * FROM store_nft ($1, $2, $3, $4, NULL, NULL, $5::TIMESTAMPTZ, ${size}::NUMERIC, ${price}::NUMERIC, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);`,
                    [symbol, name, description, image, launchDate]
                )
            } catch (e) {
                console.log(launchPad[pad])
                console.error(e)
            }
        }
    }

    if (config.allNfts) {
        let nfts = await getMeNft()
        console.log(nfts)
        for(let nft in nfts) {
            const symbol = nfts[nft].symbol
            const name = nfts[nft].name
            const description = nfts[nft].description
            const image = nfts[nft].image
            // Fix for empty or null values
            const twitter = nfts[nft].twitter ? nfts[nft].twitter : null
            const discord = nfts[nft].discord ? nfts[nft].discord : null
            const website = nfts[nft].website ? nfts[nft].website : null
            // TODO: Work on categories and upsert
            try {
                // TODO: Add support for inserting website
                const result = await client.query(
                    `SELECT * FROM store_nft ($1, $2, $3, $4, $5, $6, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);`,
                    [symbol, name, description, image, twitter, discord]
                )
            } catch (e) {
                console.log(nfts[nft])
                console.error(e)
            }
        }
    }

    if (config.discord) {
        // TODO: Fetch from DB
        const queryResult = await client.query(`SELECT nft_symbol, discord FROM nfts WHERE discord IS NOT NULL AND discord_invalid IS FALSE AND nft_id >= 749 ORDER BY nft_id ASC;`)
        
        for (const row of queryResult.rows) {
            if (!row || !row[0] || !row[1] || row[1] === 'undefined'){
                console.log(row)
                continue
            }
            await timer(3000);
            let inviteCode = row[1].toString().replace('https://discord.gg/invite/', '')
            inviteCode = inviteCode.replace('https://www.discord.gg/invite/', '')
            inviteCode = inviteCode.replace('https://www.discord.com/invite/', '')
            inviteCode = inviteCode.replace('https://discord.com/invite/', '')
            inviteCode = inviteCode.replace('https://www.discord.gg/', '')
            inviteCode = inviteCode.replace('https://www.discord.com/', '')
            inviteCode = inviteCode.replace('https://discord.com/', '')
            inviteCode = inviteCode.replace('https://discord.gg/', '')
            // TODO: Bitly?
            const nftSymbol = row[0].toString()
            console.log(nftSymbol)
            console.log(inviteCode)
            try {
                const discordData = await getDiscordStats(inviteCode)
                if(!discordData){
                    // TODO: Temp fix for invalid invite links to preven API ban
                    const flag = await client.query(
                        `UPDATE nfts SET discord_invalid = TRUE WHERE nft_symbol = $1`,
                        [nftSymbol]
                    )
                    continue
                }
                
                console.log(discordData)
                const approximate_member_count = discordData['approximate_member_count']
                const approximate_presence_count = discordData['approximate_presence_count']
                const premium_subscription_count = discordData['guild']['premium_subscription_count']
                const type = discordData['type']
                const expiresAt = discordData['expires_at']
                const guildId = discordData['guild']['id']
                const name = discordData['guild']['name']
                const splash = discordData['guild']['splash']
                const banner = discordData['guild']['banner']
                const description = discordData['guild']['description']
                const icon = discordData['guild']['icon']
                const verificationLevel = discordData['guild']['verification_level']
                const vanityUrlCode = discordData['guild']['vanity_url_code']
                const nsfw = discordData['guild']['nsfw']
                const nsfwLevel = discordData['guild']['nsfw_level']

                //TODO: Build postgres queries (2x) if it doesn't exist...
                try {
                    // TODO: Fix this for upsert on update data, but what do we want?
                    const find = await client.query(
                        `SELECT * FROM discord WHERE nft_symbol = $1`,
                        [nftSymbol]
                    )
                    if(!find || find.rows.length === 0) {
                        try {
                            const result = await client.query(
                                `INSERT INTO discord (nft_symbol, type, expires_at, guild_id, name, splash, banner, description, icon, verification_level, vanity_url_code, nsfw, nsfw_level) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);`,
                                [nftSymbol, type, expiresAt, guildId, name, splash, banner, description, icon, verificationLevel, vanityUrlCode, nsfw, nsfwLevel]
                            )
                        } catch (e) {
                            console.error(e)
                        }
                    }
                } catch (e) {
                    console.error(e)
                }
                
                // TODO: Return ID of discorb....
                const resulting = await client.query(
                    `INSERT INTO discord_stats (discord_id, nft_symbol, approximate_member_count, approximate_presence_count, premium_subscription_count) VALUES ((SELECT discord_id FROM discord WHERE nft_symbol = $1), $1, $2, $3, $4);`,
                    [nftSymbol, approximate_member_count, approximate_presence_count, premium_subscription_count]
                )
            } catch (e) {
                console.log('error')
                console.log(e)
            }
        }
    }

    if(config.twitter) {
        const queryResult = await client.query(`SELECT nft_symbol, twitter FROM nfts WHERE twitter IS NOT NULL AND nft_id >= 185 ORDER BY nft_id ASC;`)

        for (const row of queryResult.rows) {
            if(!row || !row[0] || !row[1]){
                continue
            }
            await timer(2000);
            const nftSymbol = row[0].toString()
            let twitterName = row[1].toString().replace('https://www.twitter.com/', '')
            twitterName = twitterName.replace('https://twitter.com/', '')

            try {
                const twitterData = await getTwitterStats(twitterName) as unknown as Array<object>
                if (twitterData.length === 0) {
                    continue
                }
                const twitterObject = twitterData[0] as twitterData
                console.log(twitterObject)
                const remoteId = twitterObject.id
                const screenName = twitterObject.screen_name
                const name = twitterObject.name
                const _protected = twitterObject.protected
                const followersCount = twitterObject.followers_count
                const formattedFollowersCount = twitterObject.formatted_followers_count
                const ageGated = twitterObject.age_gated
                try {
                    // TODO: Fix me for upsert / update information
                    const find = await client.query(
                        `SELECT * FROM twitter WHERE nft_symbol = $1 OR remote_id = $2`,
                        [nftSymbol, remoteId]
                    )
                    if(!find || find.rows.length === 0) {
                        try {
                            const result = await client.query(
                                `INSERT INTO twitter (nft_symbol, screen_name, name, protected, age_gated, remote_id) VALUES ($1, $2, $3, $4, $5, $6);`,
                                [nftSymbol, screenName, name, _protected, ageGated, remoteId]
                            )
                        } catch (e) {
                            console.error(e)
                        }
                    } else {
                        console.log(`Found twitter for ${screenName} already`)
                    }
                } catch(e) {
                    console.error(e)
                }
                const find2 = await client.query(
                    `SELECT * FROM twitter WHERE nft_symbol = $1`,
                    [nftSymbol, remoteId]
                )
                if(find2 && find2.rows.length > 0) {
                    const result2 = await client.query(
                        `INSERT INTO twitter_stats (twitter_id, followers_count, formatted_followers_count) VALUES ((SELECT twitter_id FROM twitter WHERE nft_symbol = $1), $2, $3);`,
                        [nftSymbol, followersCount, formattedFollowersCount]
                    )
                } else {
                    console.log(`Error updating as symbol doesn't match twitter ${screenName}`)
                }
            } catch(e) {
                console.error(e)
            }
        }
    }

    if(config.prices) {
        const queryResult = await client.query(`SELECT nft_symbol FROM nfts;`)

        for (const row of queryResult.rows) {
            if (!row || !row[0]){
                continue
            }
            await timer(1000);
            const symbol = row[0].toString()
            try {
                const priceStats = await getMeCollectionStats(symbol)

                //console.log(priceStats)
                // TODO: Create interface
                const floorPrice = priceStats?.floorPrice ? priceStats?.floorPrice : null
                const listedCount = priceStats?.listedCount ? priceStats?.listedCount : null
                const avgPrice24hr = priceStats?.avgPrice24hr ? priceStats.avgPrice24hr : null
                const volumeAll = priceStats?.volumeAll ? priceStats?.volumeAll : null

                //console.log(avgPrice24hr)
                console.log(priceStats)
                try {
                    const result = await client.query(
                        `INSERT INTO nft_market (nft_symbol, floor_price, listed_count, average_24h_price, total_volume) VALUES ($1, ${floorPrice}::NUMERIC, ${listedCount}::NUMERIC, ${avgPrice24hr}::NUMERIC, ${volumeAll}::NUMERIC)`,
                        [symbol]
                    )
                } catch(e) {
                    console.error(e)
                }
            } catch (e) {
                console.error(e)
            }
        }
    }

    console.log('Ended')
}

main()