module.exports = function () {
    var mongoose = require('mongoose');

    function isUndefined(object) { return typeof(object) === 'undefined'; }
    function create(app, config, io) {
        config = config || {};
        var connected = false,
            schema    = null,
            Tuple     = null,
            version   = config.version        || '0.0.0',
            logger    = config.logger         || {log:function(){}},
            meta      = config.meta           || {$_id:'mongo'};

        function status(next) {
            process.nextTick(function () {
                if (next) next(null, {
                    connected : connected,
                    status    : 'OK'
                });
            });
        }

        function connect(url, next) {
            mongoose.connect(url);
            hashMapSchema = new mongoose.Schema({
                                        key   : { type : String, index: true },
                                        value : String
                                         }, { autoIndex: true }
                                         );
            Tuple = mongoose.model('Tuples', hashMapSchema);

            hashSetSchema = new mongoose.Schema({
                                        key   : { type : String, index: true },
                                        members : [String]
                                         }, { autoIndex: true }
                                         );
            HashSet = mongoose.model('HashSet', hashSetSchema);
            connected = true;
        }

        function close(next) {
            mongoose.disconnect(function () {
                connected = false;
            });
        }

        function get(key, next) {
            if (key.indexOf('*') !== -1) {
                var regex  = '^' + key.replace(/\*/g, '.*') + '$',
                    promise = Tuple.find({ key : new RegExp(regex) }).exec();

                promise.then(function (documents) {
                    var tuples = documents.map(function (document) {
                        return {key: document.key, value: document.value};
                    });
                    next(null, tuples, meta);
                }, function (err) {
                    next(err, null, meta);
                }).end();
            } else {
                var promise = Tuple.findOne({ key : key }).exec();
                promise.then(function (document) {
                    next(null, document && document.value ? document.value : null, meta);
                }, function (err) {
                    next(err, null, meta);
                }).end();
            }
        }

        function set(tuples, next) {
            var count = 0;
            function update(vector, error) {
                if (vector.length === 0)
                    return next(error, count);
                var tuple = vector.shift();
                Tuple.update(
                    {key:tuple.key},
                    {key:tuple.key, value:tuple.value}, 
                    {upsert:true}, function (err, document) {
                        if (!err)
                            ++count;
                        update(vector, err ? err : error);
                });
            }
            update(tuples.slice(0));
        }

        function del(key, next) {
            var tuples = [];
            if (key instanceof Array)
                tuples = key.map(function (oneKey) {return {key:oneKey}});
            else if (key.indexOf('*') !== -1)
                tuples = [{key:new RegExp('^' + key.replace(/\*/g, '.*') + '$')}];
            else
                tuples = [{key:key}];

            var count = 0;
            function del(vector, error) {
                if (vector.length === 0)
                    return next(error, count);
                var tuple = vector.shift();
                Tuple.find().remove(tuple, function (err, document) {
                    if (!err)
                        ++count;
                    del(vector, err ? err : error);
                });
            }
            del(tuples.slice(0));
        }

        function sget(setName, member, next) {
            if (member === '*') {
                var promise = HashSet.findOne({ key : setName }).exec();
                promise.then(function (document) {
                    next(null, document && document.members ? document.members : [], meta);
                }, function (err) {
                    next(err, null, meta);
                }).end();
            } else {
                var promise = HashSet.findOne({ key : setName, members : member }).exec();
                promise.then(function (document) {
                    logger.log('debug', 'mongo::sget(setName: %s, member:%s) >>> document:%j', setName, member, document, meta);
                    next(null, document ? 1 : 0, meta);
                }, function (err) {
                    next(err, null, meta);
                }).end();
            }
        }

        function sadd(tuples, next) {
            var count = 0;
            logger.log('debug', 'mongo::sadd(tuples: %j)', tuples, meta);
            function update(vector, error) {
                if (vector.length === 0)
                    return next(error, count);
                var tuple = vector.shift();
                logger.log('debug', 'mongo::sadd(setName:%s, member:%s)', tuple.key, tuple.value, meta);
                HashSet.update(
                    {key:tuple.key},
                    {$addToSet:{members:tuple.value}}, 
                    {upsert:true}, function (err, document) {
                        logger.log('debug', 'mongo::sadd(setName:%s, member:%s) >>> document:%j', tuple.key, tuple.value, document, meta);
                        if (!err)
                            ++count;
                        update(vector, err ? err : error);
                });
            }
            update(tuples.slice(0));
        }
        
        function sdel(setName, member, next) {
            var count = 0;
                // delete the whole set
            if (member === '*') {
                var promise = HashSet.remove({ key : setName }).exec();
                promise.then(function (document) {
                    next(null, 1, meta);
                }, function (err) {
                    next(err, null, meta);
                }).end();
            }
            else {
                var promise = HashSet.update({ key : setName }, { $pull : { members : member } }).exec();
                promise.then(function (document) {
                    logger.log('debug', 'mongo::sdel(setName:%s, member:%s) >>> document:%j', setName, member, document, meta);
                    next(null, 1, meta);
                }, function (err) {
                    next(err, null, meta);
                }).end();
            }
            
        }

        ////////////////////////////////////////////////////////////////////////

        return {
            constructor : function (next) {
                connect(process.env.MONGODB_URL || process.env.MONGOLAB_URI || config.plugins.store.url, next);
            },
            shutdown : function (next) {
                close(next);
            },
            status  : status,
            connect : connect,
            close   : close,
            get     : get,
            set     : set,
            del     : del,
            sadd    : sadd,
            sget    : sget,
            sdel    : sdel
        };
    }

    return create;
}();
