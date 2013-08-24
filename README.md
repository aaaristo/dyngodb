dyngodb
=======

An experiment to get a [MongoDB](http://www.mongodb.org/) *like* interface in front of [DynamoDB](http://aws.amazon.com/dynamodb/)
and [CloudSearch](http://aws.amazon.com/cloudsearch/).

## Why?

DynamoDB is *elastic*, *cheap* and greatly integrated with many AWS products (e.g. [Elastic MapReduce](http://aws.amazon.com/elasticmapreduce/),
[Redshift](http://aws.amazon.com/redshift/),[Data Pipeline](http://aws.amazon.com/datapipeline/),[S3](http://aws.amazon.com/s3/)),
while MongoDB has a wonderful interface. Using node.js on [Elastic Beanstalk](http://aws.amazon.com/elasticbeanstalk/)
and DynamoDB as your backend you could end-up with a very scalable, cheap and high available webapp architecture.
The main stop on it for many developers would be being able to productively use DynamoDB, hence this project.

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
> john= last
> john.city= 'London'
> db.test.save(john)
> db.test.find({ name: 'John' })
> db.test.ensureIndex({ name: 'S' })
> db.test.findOne({ name: 'John' })
> db.test.ensureIndex({ $search: { domain: 'mycstestdomain', lang: 'en' } }); /* some CloudSearch */
> db.test.update({ name: 'John' },{ $set: { city: 'Boston' } });
> db.test.find({ $search: { q: 'Boston' } });
> db.test.find({ name: 'Jane' }) /* some graphs */
> jane= last
> jane.houseband= john
> john.wife= jane
> john.himself= john
> db.test.save(john);
> db.test.save(jane);
> db.test.remove()
> db.test.drop()
</pre>

## Goals

FIRST

* support a MongoDB *like* query language

* support slice and dice, Amazon EMR and be friendly to tools that integrates with DynamoDB
  (so no compression of JSON objects for storage)

* support graphs, and respect object identity

* prevent lost-updates

THEN

* support transactions ([DynamoDB Transactions](https://github.com/awslabs/dynamodb-transactions))

## What dyngodb actually does

* Basic find() support (basic operators, no $all, $and $or..., some projection capabilities):
  finds are implemented via 3 components: 
      * parser: parses the query and produce a "query" object that is 
                used to track the state of the query from its beginning to the end.

      * finder: tries to retrive less-possible data from DynamoDB in the fastest way

      * refiner: "completes" the query, doing all the operations that finder was not able
                 to perform (for lack of support in DynamoDB or because i simply 
                 haven't found a better way).

* Basic save() support: DynamoDB does not support sub-documents. So the approach here is to
  save sub-documents as documents of the table and link them to the parent object like this:

       <pre>
       db.test.save({ name: 'John', wife: { name: 'Jane' } }) 
       => 2 items inserted into the test table
       1:      { $id: '50fb5b63-8061-4ccf-bbad-a77660101faa',
                 name: 'John',
                 $$wife: '028e84d0-31a9-4f4c-abb6-c6177d85a7ff' }
       2:      { $id: '028e84d0-31a9-4f4c-abb6-c6177d85a7ff',
                 name: 'Jane' }
       </pre>
       
       where $id is the HASH of the DynamoDB table. This enables us to respect the javascript object 
       identity as it was in memory, and you will get the same structure - even if it where a cyrcular graph -
       (actually with some addons $id, $version...) when you query the data out:

       db.test.find({ name: 'John' }) => will SCAN for name: 'John' return the first object, detects $$wife
       ($$ for an object, $$$ for an [array](#arrays)) and get (getItem) the second object. Those meta-attributes are keeped
       in the result for later use in save().

* Basic update() support: $set, $unset (should add $push and $pull)

* Basic lost update prevention

### Finders

There are 3 types of finders actually (used in this order):

* Simple: manage $id queries, so the ones where the user specify the HASH of the DynamoDB table

* Indexed: tries to find an index that is able to find hashes for that query

* Scan: fails back to [Scan](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html)
  the table :(, that you should try to avoid probably indexing fields,
  or changing some design decision.

### Indexing

Indexes in dyngodb are DynamoDB tables that has a different KeySchema, and contains the data needed
to lookup items based on some attributes. This means that typically an index will be used with a
[Query](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html) operation.

There are actually 2 indexes (4 but only 2 are used):

* [fat](http://dictionary.reference.com/browse/fat).js: as the name suggests it is a pretty heavy 
  "general purpose" index that will generate many additional writes: 1 for every field indexed + 1.
  Lets see an example:

  Suppose to have a table like this:
  <pre>
  { type: 'person', category: 'hipster', name: 'Jane', company: 'Acme' }
  { type: 'person', category: 'hacker', name: 'Jack', city: 'Boston' }
  { type: 'person', category: 'hustler', name: 'John', country: 'US' }
  { type: 'company', category: 'enterprise', name: 'Amazon', phone: '13242343' }
  { type: 'company', category: 'hipster', name: 'Plurimedia' }
  </pre>
  
  And an index like:
  <pre>
  db.test.ensureIndex({ type: 'S', category: 'S', name: 'S' });  
  </pre>
  
  The index will be used in queries like:
  <pre>
    db.test.find({ type: 'person' }).sort({ name: -1 })
    db.test.find({ type: 'person', category: 'hipster' })
  </pre>
  
  and will NOT be used in query like this
  <pre>
    db.test.find({ name: 'Jane' })
    db.test.find({ category: 'hipster', name: 'Jane' })
    db.test.find().sort({ name: -1 })
  </pre>

  and will be used partially (filter on type only) for this queres:
  <pre>
    db.test.find({ type: 'person', name: 'Jane' })
  </pre>
  
  So columns are ordered in the index and you can only use it starting with the first
  and attaching the others as you defined it in ensureIndex() with an EQ operator or
  the query (finder) will use the index until the first non EQ operator and then the refiner
  will filter/sort the rest. Local secondary indexes are created an all indexed attributes,
  to support non-EQ operators, that means that actually you can index only 5 attributes with
  this kind of index.

* cloud-search.js: is a fulltext index using AWS CloudSearch under the covers.

  Suppose to have the same table as before.
  And an index like:
  <pre>
  db.test.ensureIndex({ $search: { domain: 'test', lang: 'en' } });  
  </pre>

  You can then search the table like this:
  <pre>
  db.test.find({ $search: { q: 'Acme' } });
  db.test.find({ $search: { bq: "type:'contact'", q: 'John' } });
  </pre>


* you could probably build your own specialized indexes too.. just copy the fat.js index and
  add the new your.js index to the indexed.js finder at the top of indexes array.
  (probably we should give this as a configuration option)


### Lost update prevention

Suppose to have two sessions going on

Session 1 connects and read John
<pre>
$ dyngodb
> db.test.find({ name: 'John' })
</pre>

Session 2 connects and read John
<pre>
$ dyngodb
> db.test.find({ name: 'John' })
</pre>

Session 1 modifies and saves John
<pre>
> last.city= 'San Francisco'
> db.test.save(last)
done!
</pre>

Session 2 modifies and tries to save John and gets an error
<pre>
> last.country= 'France'
> db.test.save(last)
The item was changed since you read it
</pre>

This is accomplished by a $version attribute which is incremented
at save time if changes are detected in the object since it was read
($old attribute contains a clone of the item at read time).
So when Session 2 tries to save the object it tries to save it
[expecting](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#DDB-PutItem-request-Expected) the item to have $old.$version in the table and it fails
because Session 1 already incremented it.

### Arrays

Actually dyngodb is pretty incoherent about arrays, infact it has two kinds of array persistence: 

* DynamoDB supports sets which are basically javascript _unordered_ arrays of strings or numbers or binary data,
  so if dyngodb detects an array of one of those types it persists it like a set (hence loosing its order):

  <pre>
    db.test.save({ name: 'John', tags: ['developer','hipster','hacker','cool'] })
  </pre>


* Object arrays _are kept in order_ (see [Schema](#schema)): 

  <pre>
    db.test.save({ name: 'John', sons: [{ name: 'Konrad' },{ name: 'Sam' },{ name: 'Jill' }] })
  </pre>
  
  this is accomplished via the $pos RANGE attribute of the collection table. So saving the object above
  would result in 4 items inserted in the DynamoDB table where 2 HASHes are generated (uuid):
  
  <pre>
  1. { $id: 'uuid1', $pos: 0, name: 'John', $$$sons: 'uuid2' }
  2. { $id: 'uuid2', $pos: 0, name: 'Konrad' }
  3. { $id: 'uuid2', $pos: 1, name: 'Sam' }
  4. { $id: 'uuid2', $pos: 2, name: 'Jill' }
  </pre>

* Arrays of arrays or other type of: they don't work. actually never tested it.

  <pre>
    db.test.save({ name: 'John', xxx: [[{},{}],[{}],[{}]] })
    db.test.save({ name: 'John', xxx: [{},[{}],2] })
  </pre>

### Schema

In dyngodb you have 2 DynamoDB table KeySchema:

* the one used for collections where you have $id (string) as the HASH attribute and $pos (number) as the range attribute.
  $id, if not specified in the object, is autogenerated with an UUID V4. $pos is always 0 for objects not contained in
  an array, and is the position of the object in the array for objects contained in arrays (see [Arrays](#arrays)).

* the one used for indexes where you have $hash (string) as HASH attribute and $range (string) as the range attribute.
  $hash represents probably the container of the results for a certain operator. and $range is used to keep the key
  attributes of the results ($id+':'+$pos).

