WITH nft_base AS (
	SELECT * FROM nfts
), discord_max AS (
	SELECT DISTINCT ON (discord_id)
	*
	FROM discord_stats
	ORDER BY discord_id, created_at DESC
), discord_min AS (
	SELECT DISTINCT ON (discord_id)
	*
	FROM discord_stats
	ORDER BY discord_id, created_at ASC
), twitters_max AS (
	SELECT DISTINCT ON (twitter_id)
	*
	FROM twitter_stats
	ORDER BY twitter_id, created_at DESC
), twitters_min AS (
	SELECT DISTINCT ON (twitter_id)
	*
	FROM twitter_stats
	ORDER BY twitter_id, created_at ASC
), account_details AS (
	SELECT
	*
	FROM twitter
),market_stats AS (
	SELECT DISTINCT ON (nft_symbol)
	*
	FROM nft_market
	ORDER BY nft_symbol, created_at DESC
)
SELECT
	nft_base.name,
	nft_base.twitter,
	nft_base.discord,
	nft_base.mint_size,
	nft_base.mint_price,
	nft_base.mint_date,
	discord_max.approximate_member_count AS discord_members,
	discord_max.approximate_presence_count AS discord_active,
	discord_max.premium_subscription_count AS discord_boosts,
	twitters_max.followers_count AS twitter_follows,
	market_stats.floor_price / 10 ^ 9 AS floor_price,
	market_stats.listed_count,
	market_stats.average_24h_price / 10 ^ 9 AS avg_24h_price,
	market_stats.total_volume / 10 ^ 9 AS total_volume, 
	NULLIF(market_stats.listed_count, 0) / NULLIF(nft_base.mint_size, 0) AS listed_pct
FROM nft_base
JOIN market_stats ON nft_base.nft_symbol = market_stats.nft_symbol
LEFT JOIN discord_nfts ON nft_base.nft_symbol = discord_nfts.nft_symbol
LEFT JOIN twitter_nfts ON nft_base.nft_symbol = twitter_nfts.nft_symbol
LEFT JOIN twitters_max ON twitter_nfts.twitter_id = twitters_max.twitter_id
LEFT JOIN discord_max ON discord_nfts.discord_id = discord_max.discord_id
WHERE market_stats.total_volume IS NOT NULL;


WITH vaid_discords AS (
    SELECT *
    FROM nfts
    WHERE discord_invalid IS FALSE
    AND discord IS NOT NULL
), latest_update AS (
    SELECT
    DISTINCT ON (discord_stats.nft_symbol)
    vaid_discords.nft_id AS nft_id,
    vaid_discords.nft_symbol AS nft_symbol,
    discord_stats.created_at AS created_at
    FROM vaid_discords
    LEFT JOIN discord_stats ON discord_stats.nft_symbol = vaid_discords.nft_symbol
    ORDER BY discord_stats.nft_symbol, discord_stats.created_at DESC
), filter_recents AS (
    SELECT nft_id
    FROM latest_update
    WHERE created_at IS NULL
    OR created_at > NOW() - INTERVAL '3 hours'
)
SELECT nft_symbol, discord FROM vaid_discords WHERE nft_id NOT IN (SELECT * FROM filter_recents);

-- Discord Join
WITH generic_nft AS (
	SELECT * FROM nfts WHERE discord IS NOT NULL AND discord_invalid IS FALSE
), join_nfts AS (
	SELECT
	generic_nft.discord,
	discord.nft_symbol AS discord_symbol,
	discord.discord_id,
	nfts.nft_symbol
	FROM generic_nft
	LEFT JOIN nfts ON nfts.discord = generic_nft.discord
	LEFT JOIN discord ON discord.nft_symbol = generic_nft.nft_symbol
	WHERE nfts.nft_symbol != generic_nft.nft_symbol
	--AND discord.nft_symbol IS NOT NULL
)
SELECT * FROM join_nfts; 
--INSERT INTO discord_nfts (discord_id, nft_symbol) SELECT discord_id, nft_symbol FROM join_nfts;

-- Twitter Join
WITH generic_nft AS (
	SELECT * FROM nfts WHERE twitter IS NOT NULL
), join_nfts AS (
	SELECT
		generic_nft.twitter,
		twitter.nft_symbol AS twitter_symbol,
		twitter.twitter_id,
		nfts.nft_symbol
	FROM generic_nft
	LEFT JOIN nfts ON nfts.twitter = generic_nft.twitter
	LEFT JOIN twitter ON twitter.nft_symbol = generic_nft.nft_symbol
	WHERE nfts.nft_symbol != generic_nft.nft_symbol
	AND twitter.nft_symbol IS NOT NULL
)
SELECT * FROM join_nfts;
--INSERT INTO twitter_nfts (twitter_id, nft_symbol) SELECT twitter_id, nft_symbol FROM join_nfts;

WITH current_discord AS (
	SELECT discord_id, nft_symbol FROM discord
)
INSERT INTO discord_nfts (discord_id, nft_symbol) SELECT * FROM current_discord;


WITH current_twitter AS (
	SELECT twitter_id, nft_symbol FROM twitter
)
INSERT INTO twitter_nfts (twitter_id, nft_symbol) SELECT * FROM current_twitter;