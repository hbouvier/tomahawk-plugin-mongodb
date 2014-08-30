var store = require('../lib/store.js')();

store.connect('mongodb://localhost/test');

store.set('name', 'Bob Smith', function (err, result) {
   store.get('name', function (err, document) {
      console.log('document:', document);
      store.close();
   });
});
