module.exports = function () {
    var mongoose = require('mongoose');

    function create(app, config, io) {
        if (!config || !config.store) config = {store:{}};
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
            schema = new mongoose.Schema({
                                        key   : { type : String, index: true },
                                        value : String
                                         }, { autoIndex: true }
                                         );
//           schema.index({ key: 1, type: -1 });
           Tuple = mongoose.model('Tuples', schema);
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

        function set(key, value, next) {
            console.log('key:',key, ', value:', value);
            var promise = Tuple.update({ key : key }, { $set : { value : value }}, {upsert: true}).exec();

            promise.then(function (document) {
                console.log('key:',key, ', value:', value, ' >>> ', document);
                next(null, 'OK', meta);
            }, function (err) {
                console.log('key:',key, ', value:', value, ' >>> err:', err);
                next(err, null, meta);
            }).end();
        }

        function del(key, next) {
            var keys = key instanceof Array ? key : [key];
            var tuples = keys.map(function (oneKey) {
                return { key : oneKey };
            });
            Tuple.collection.del(tuples)
            promise.then(function (document) {
                next(null, 'OK', meta);
            }, function (err) {
                next(err, null, meta);
            }).end();
        }

        ////////////////////////////////////////////////////////////////////////
        return {
            status  : status,
            connect : connect,
            close   : close,
            get     : get,
            set     : set,
            del     : del
        };
    }

    return create;
}();
