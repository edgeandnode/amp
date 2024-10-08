        SELECT
            param_pair                AS pool,
            'x' || '/' || 'x'  AS _symbol,
            'Uniswap V2 Pool: ' || 'x' AS name
        FROM events_factory_contract_pair_created