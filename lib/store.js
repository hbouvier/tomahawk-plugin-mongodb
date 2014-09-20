module.exports = function () {
    var mongoose = require('mongoose');

    function create(app, config, io) {
        var connected = false,
            schema    = null,
            Tuple     = null,
            meta      = {$_id:'mongo'};

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
//           schema.index({ key: 1, type: -1 });
           Tuple = mongoose.model('Tuples', hashMapSchema);
//           Tuple.ensureIndexes(function (err) { console.log('err:', err); });

            hashSetSchema = new mongoose.Schema({
                                        key   : { type : String, index: true },
                                        members : [String]
                                         }, { autoIndex: true }
                                         );
//           schema.index({ key: 1, type: -1 });
           HashSet = mongoose.model('HashSet', hashSetSchema);
//           Tuple.ensureIndexes(function (err) { console.log('err:', err); });

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
                    next(null, document && document.value, meta);
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
            /*
            var keys   = tuples.map(function (tuple) {return {key:tuple.key}});
            var values = tuples.map(function (tuple) {return {key:tuple.key,value:tuple.value }});
            var total   = 0,
                failed  = 0,
                errors  = [];
            console.log('keys:', keys, ', values:',values);
            Tuple.update(keys, values, {upsert: true}, function (err, doc) {
                console.log('set err:', err, ', doc:', doc);
                ++total;
                if (err)
                    ++failed;
                errors.push(err);

                if (total === tuples.length) {
                    if (failed !== 0) {
                        return next(total === 1 ? errors[0] : errors, null, meta);
                    }
                    next(null, 'OK', meta);
                }
            });
*/
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

            /*
                        var total   = 0,
                failed  = 0,
                errors  = [];
            Tuple.remove(tuples, function (err, doc) {
                ++total;
                if (err)
                    ++failed;
                errors.push(err);

                if (total === tuples.length) {
                    if (failed !== 0) {
                        return next(total === 1 ? errors[0] : errors, null, meta);
                    }
                    next(null, total, meta);
                }
            });
*/



        }

        function sget(setName, member, next) {
            if (typeof(member) === 'undefined') {
                var promise = HashSet.findOne({ key : setName }).exec();
                promise.then(function (document) {
                    next(null, document && document.members, meta);
                }, function (err) {
                    next(err, null, meta);
                }).end();
            } else {
                var promise = HashSet.findOne({ key : setName, members : member }).exec();
                promise.then(function (document) {
                    next(null, document ? 1 : 0, meta);
                }, function (err) {
                    next(err, null, meta);
                }).end();
            }
        }

        function sadd(tuples, next) {
            var count = 0;
            function update(vector, error) {
                if (vector.length === 0)
                    return next(error, count);
                var tuple = vector.shift();
                Tuple.update(
                    {key:tuple.key},
                    {$addToSet:{members:tuple.value}}, 
                    {upsert:true}, function (err, document) {
                        if (!err)
                            ++count;
                        update(vector, err ? err : error);
                });
            }
            update(tuples.slice(0));

            /*
            var keys    = tuples.map(function (tuple) {return {key:tuple.key}});
            var members = tuples.map(function (tuple) {return { $addToSet : { members : tuple.value }}});
            var total   = 0,
                failed  = 0,
                errors  = [];
            console.log("keys:", keys, ', values:', members);
            HashSet.update(keys, values, {upsert: true}, function (err, doc) {
                ++total;
                if (err)
                    ++failed;
                errors.push(err);

                if (total === tuples.length) {
                    if (failed !== 0) {
                        return next(total === 1 ? errors[0] : errors, null, meta);
                    }
                    next(null, 'OK', meta);
                }
            });
*/
        }
        
        function sdel(setName, member, next) {
            var count = 0;
                // delete the whole set
            if (typeof(member) === 'undefined') {
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
