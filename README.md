# REST API, Key Value Pair Store, backed by MongoDB for Tomahawk

## To use this plugin

    npm install -g tomahawk-routes-kv-store
    npm install -g tomahawk-plugin-mongo

Then create a configuration file in your home directory:

    ~/.tomahawk/config.json
    {
        "plugins" : {
            "store" : {
                "context"        : "/store/api/v1",
                "interval"       : 1000,
                "implementation" : "tomahawk-plugin-mongo",
                "url"            : "mongodb://localhost:27017/tomahawk"
            },
            "store-route" : {
                "implementation" : "tomahawk-routes-kv-store"
            }
        }
    }