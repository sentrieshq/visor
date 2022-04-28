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

CREATE FUNCTION store_activity (
    _signature TEXT,
    _type TEXT,
    _source TEXT,
    _token_mint TEXT,
    _nft_symbol TEXT,
    _slot BIGINT,
    _blocktime BIGINT,
    _buyer TEXT DEFAULT NULL,
    _buyer_referral TEXT DEFAULT NULL,
    _seller TEXT DEFAULT NULL,
    _seller_referral TEXT DEFAULT NULL,
    _price NUMERIC DEFAULT NULL
)
RETURNS TABLE ("activity_id" BIGINT, "nft_symbol" TEXT, "type" TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    _activity_id BIGINT;
    __signature TEXT;
    __type TEXT;
    __source TEXT;
    __token_mint TEXT;
    __nft_symbol TEXT;
    __slot BIGINT;
    __blocktime BIGINT;
    __buyer TEXT;
    __buyer_referral TEXT;
    __seller TEXT;
    __seller_referral TEXT;
    __price NUMERIC;
    _created_at TIMESTAMPTZ;
BEGIN
    BEGIN
        SELECT 
            n.activity_id,
            n.signature,
            n.type,
            n.source,
            n.token_mint,
            n.nft_symbol,
            n.slot,
            n.blocktime,
            n.buyer,
            n.buyer_referral,
            n.seller,
            n.seller_referral,
            n.price,
            n.created_at
        INTO STRICT
            _activity_id,
            __signature,
            __type,
            __source,
            __token_mint,
            __nft_symbol,
            __slot,
            __blocktime,
            __buyer,
            __buyer_referral,
            __seller,
            __seller_referral,
            __price,
            _created_at
        FROM nft_activity n
        WHERE n.nft_symbol = _nft_symbol
        AND n.signature = _signature
        AND n.type = _type;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
            NULL;
    END;
    IF _activity_id IS NULL THEN
        BEGIN
            INSERT INTO nft_activity (
                signature,
                type,
                source,
                token_mint,
                nft_symbol,
                slot,
                blocktime,
                buyer,
                buyer_referral,
                seller,
                seller_referral,
                price
            )
            VALUES (
                _signature,
                _type,
                _source,
                _token_mint,
                _nft_symbol,
                _slot,
                _blocktime,
                _buyer,
                _buyer_referral,
                _seller,
                _seller_referral,
                _price
            )
            -- REVIEW
            RETURNING
                nft_activity.activity_id, nft_activity.nft_symbol, nft_activity.type
            INTO STRICT _activity_id, __nft_symbol, __type;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    RAISE EXCEPTION 'Failed to insert activity: %, %, %', _activity_id, _nft_symbol, _type;
        END;
    ELSE
        BEGIN
            UPDATE nft_activity
            SET updated_at = NOW()
            WHERE nft_activity.activity_id = _activity_id
            -- REVIEW
            RETURNING
                nft_activity.activity_id, nft_activity.nft_symbol, nft_activity.type
            INTO STRICT _activity_id, __nft_symbol, __type;
            EXCEPTION WHEN NO_DATA_FOUND THEN
                RAISE EXCEPTION 'Failed to insert activity: %, %, %', _activity_id, _nft_symbol, _type;
        END;
    END IF;
    RETURN QUERY SELECT _activity_id, _nft_symbol, _type;
END;
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