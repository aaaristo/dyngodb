dyngodb
=======

An experiment to get a MongoDB like interface in front of DynamoDB

## Getting started
Playing around:
<pre>
$ npm install -g dyngodb
</pre>
<pre>
$ dyngodb
> db.createCollection('test')
> db.test.save({ name: 'John', lname: 'Smith' })
> db.test.save({ name: 'Jane', lname: 'Burden' })
> db.test.find({ name: 'John' })
> last
> last.city= 'London'
> db.test.save(last)
> db.test.find({ name: 'John' })
> db.test.remove()
> db.test.drop()
</pre>
