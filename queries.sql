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


--INSERT INTO twitter_nfts (twitter_id, nft_symbol)
WITH with_discord AS (
SELECT * FROM nfts WHERE discord IS NOT NULL
), join_discords AS (
	SELECT
		with_discord.nft_symbol AS filtered,
		nfts.nft_symbol AS matching,
		nfts.discord
	FROM nfts
	JOIN with_discord ON with_discord.discord = nfts.discord
	WHERE nfts.nft_symbol != with_discord.nft_symbol
), missing_discord AS (
	SELECT 
		filtered AS m_d_filtered,
		matching AS m_d_matching,
		join_discords.discord AS m_d_discord
	FROM join_discords LEFT JOIN discord_nfts ON discord_nfts.nft_symbol = join_discords.filtered WHERE discord_nfts.discord_id IS NULL
), has_discord AS (
	SELECT
		filtered AS h_d_filtered,
		matching AS h_d_matching,
		join_discords.discord AS h_d_discord,
		discord_nfts.discord_id AS discord_id
	FROM join_discords LEFT JOIN discord_nfts ON discord_nfts.nft_symbol = join_discords.matching WHERE discord_nfts.discord_id IS NOT NULL
), join_two AS (
	SELECT * FROM missing_discord JOIN has_discord ON has_discord.h_d_discord = missing_discord.m_d_discord
)
--INSERT INTO discord_nfts (discord_id, nft_symbol) 
SELECT discord_id, h_d_filtered FROM join_two WHERE h_d_filtered NOT IN (SELECT nft_symbol FROM discord_nfts) GROUP BY discord_id, h_d_filtered;


WITH with_twitter AS (
	SELECT * FROM nfts WHERE twitter IS NOT NULL
), join_twitter AS (
	SELECT
		with_twitter.nft_symbol AS filtered,
		nfts.nft_symbol AS matching,
		nfts.twitter
	FROM nfts
	JOIN with_twitter ON with_twitter.twitter = nfts.twitter
	WHERE nfts.nft_symbol != with_twitter.nft_symbol
), missing_twitter AS (
	SELECT 
		filtered AS m_d_filtered,
		matching AS m_d_matching,
		join_twitter.twitter AS m_d_twitter
	FROM join_twitter LEFT JOIN twitter_nfts ON twitter_nfts.nft_symbol = join_twitter.filtered WHERE twitter_nfts.twitter_id IS NULL
), has_twitter AS (
	SELECT
		filtered AS h_d_filtered,
		matching AS h_d_matching,
		join_twitter.twitter AS h_d_twitter,
		twitter_nfts.twitter_id AS twitter_id
	FROM join_twitter LEFT JOIN twitter_nfts ON twitter_nfts.nft_symbol = join_twitter.matching WHERE twitter_nfts.twitter_id IS NOT NULL
), join_two AS (
	SELECT * FROM missing_twitter JOIN has_twitter ON has_twitter.h_d_twitter = missing_twitter.m_d_twitter
)
--INSERT INTO twitter_nfts (twitter_id, nft_symbol) 
SELECT twitter_id, h_d_filtered FROM join_two WHERE h_d_filtered NOT IN (SELECT nft_symbol FROM twitter_nfts) GROUP BY twitter_id, h_d_filtered;


SELECT
nft_symbol,
COUNT(*) AS total_72h_sales,
AVG(price)
FROM nft_activity
WHERE buyer IS NOT NULL and seller IS NOT NULL
AND type = 'buyNow' AND TO_TIMESTAMP(blocktime) < NOW() - INTERVAL '72 hours'
GROUP BY nft_symbol ORDER BY COUNT(*) DESC;