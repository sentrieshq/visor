CREATE FUNCTION store_nft (
    _nft_symbol TEXT,
    _name TEXT,
    _description TEXT,
    _image TEXT,
    _twitter TEXT DEFAULT NULL, -- Can be null
    _discord TEXT DEFAULT NULL, -- Can be null
    _mint_date TIMESTAMPTZ DEFAULT NULL, -- Can be null
    _mint_size NUMERIC DEFAULT NULL, -- Can be null
    _mint_price NUMERIC DEFAULT NULL, -- Can be null
    _code TEXT DEFAULT NULL,
    _has_issue BOOLEAN DEFAULT NULL,
    _notes TEXT DEFAULT NULL,
    _chain TEXT DEFAULT NULL,
    _has_staking BOOLEAN DEFAULT NULL,
    _has_token BOOLEAN DEFAULT NULL,
    _is_doxxed BOOLEAN DEFAULT NULL,
    _mint_marketplace_id BIGINT DEFAULT NULL
) 
RETURNS TABLE ("nft_id" BIGINT, "name" TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    _nft_id BIGINT;
    __nft_symbol TEXT;
    __name TEXT;
    __description TEXT;
    __image TEXT;
    __twitter TEXT;
    __discord TEXT;
    __mint_size NUMERIC;
    __mint_price NUMERIC;
    __mint_date TIMESTAMPTZ;
BEGIN
    BEGIN
        SELECT 
            n.nft_id,
            n.nft_symbol,
            n.name,
            n.description,
            n.image,
            n.twitter,
            n.discord,
            n.code,
            n.mint_size,
            n.mint_price,
            n.mint_date,
            n.has_issue,
            n.notes,
            n.chain,
            n.has_staking,
            n.has_token,
            n.is_doxxed,
            n.mint_marketplace_id
        INTO STRICT
            _nft_id,
            __nft_symbol,
            __name,
            __description,
            __image,
            __twitter,
            __discord,
            _code,
            __mint_size,
            __mint_price,
            __mint_date,
            _has_issue,
            _notes,
            _chain,
            _has_staking,
            _has_token,
            _is_doxxed,
            _mint_marketplace_id
        FROM nfts n
        WHERE n.nft_symbol = _nft_symbol
        AND n.name = _name;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
            NULL;
    END;
    IF _nft_id IS NULL THEN
        BEGIN
            INSERT INTO nfts (
                nft_symbol,
                name,
                description,
                image,
                twitter,
                discord,
                code,
                mint_size,
                mint_price,
                mint_date,
                has_issue,
                notes,
                chain,
                has_staking,
                has_token,
                is_doxxed,
                mint_marketplace_id
            )
            VALUES (
                _nft_symbol,
                _name,
                _description,
                _image,
                _twitter,
                _discord,
                _code,
                _mint_size,
                _mint_price,
                _mint_date,
                _has_issue,
                _notes,
                _chain,
                _has_staking,
                _has_token,
                _is_doxxed,
                _mint_marketplace_id
            )
            RETURNING nfts.nft_id INTO STRICT _nft_id;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    RAISE EXCEPTION 'Failed to insert nft: %, %', _nft_id, __name;
        END;
    ELSE
        IF __mint_size IS NULL THEN
            __mint_size := _mint_size;
        END IF;

        IF __mint_date IS NULL THEN 
            __mint_date := _mint_date;
        END IF;

        IF __mint_price IS NULL THEN
            __mint_price := _mint_price;
        END IF;

        IF __twitter IS NULL THEN
            __twitter := _twitter;
        END IF;

        IF __discord IS NULL THEN
            __discord := _discord;
        END IF;

        BEGIN
            UPDATE nfts
            SET
                mint_size = __mint_size,
                mint_date = __mint_date,
                mint_price = __mint_price,
                twitter = __twitter,
                discord = __discord
            WHERE nfts.nft_id = _nft_id
            RETURNING nfts.nft_id INTO STRICT _nft_id;
            EXCEPTION WHEN NO_DATA_FOUND THEN
                RAISE EXCEPTION 'Failed to update nft: %, %', _nft_id, _name;
        END;
    END IF;
    RETURN QUERY SELECT _nft_id, _name;
END;
$$;

CREATE FUNCTION store_twitter (

)
$$
$$;

CREATE FUNCTION store_discord (

)
$$
$$;


-- TODO: Fix
CREATE FUNCTION fetch_discords_pending_update ()
RETURNS TABLE ("nft_id" BIGINT, "discord" TEXT)
LANGUAGE plpgsql
AS $$
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
	OR created_at > NOW() - INTERVAL '1 hours'
)
SELECT nft_id, discord FROM vaid_discords WHERE nft_id NOT IN (SELECT * FROM filter_recents)
RETURNING nft_id, discord;
$$;