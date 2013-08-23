dyngodb
=======

An experiment to get a [MongoDB](http://www.mongodb.org/) like interface in front of [DynamoDB](http://aws.amazon.com/dynamodb/)
and [CloudSearch](http://aws.amazon.com/cloudsearch/)

## Getting started
Playing around:
<pre>
$ npm install -g dyngodb
</pre>
<pre>
$ export AWS_ACCESS_KEY_ID=......
$ export AWS_SECRET_ACCESS_KEY=......
$ export AWS_REGION=eu-west-1
$ dyngodb
> db.createCollection('test')
> db.test.save({ name: 'John', lname: 'Smith' })
> db.test.save({ name: 'Jane', lname: 'Burden' })
> db.test.find({ name: 'John' })
> last
> last.city= 'London'
> db.test.save(last)
> db.test.find({ name: 'John' })
> db.test.ensureIndex({ name: 'S' })
> db.test.findOne({ name: 'John' })
> db.prova.ensureIndex({ $search: { domain: 'mycstestdomain', lang: 'en' } }); /* some CloudSearch */
> db.test.update({ name: 'John' },{ $set: { city: 'Boston' } });
> db.prova.find({ $search: { q: 'Boston' } });
> db.test.remove()
> db.test.drop()
</pre>
