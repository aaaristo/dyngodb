var _= require('underscore'),
    zlib = require('zlib'),
    async = require('async'),
    argv = require('optimist').argv,
    debug = argv['dyn-debug'],
    Stream= require('stream').Stream,
    configureTransaction= require('./dyn.tx.js'),
    AWS = require('aws-sdk');

const _catch= function (fn)
      {
         return function ()
         {
             try
             {
                 fn.apply(null,arguments);
             }
             catch (ex)
             {
                console.log(ex,ex.stack);
             }
         };
      },
      _arr= function (val)
      {
          return Array.isArray(val) ? val : [val];
      },
      _value= function (val)
      {
          var type= typeof val;

          if (type=='object'&&val instanceof Buffer)
            return { 'B': val.toString('base64') };
          else
          if (type=='object'&&val instanceof Date)
            return { 'S': val.toISOString() };
          else
          if (type=='object'&&Array.isArray(val))
          {
              if (val.length>0)
              {
                  var etype= typeof val[0];

                  if (etype=='object'&&val[0] instanceof Buffer)
                    return { 'BS': _.collect(val,function (v) { return v.toString('base64') }) };
                  else
                  if (etype=='number')
                    return { 'NS': _.collect(val,function (v) { return v+''; }) };
                  else
                  if (etype=='string')
                    return { 'SS': _.collect(val,function (v) { return v+''; }) };
                  else
                    throw new Error('unknown type of array value: '+etype);
              }
              else
                  throw new Error('empty array');
          }
          else
          if (type=='number')
            return { 'N': val+'' };
          else
          if (type=='string')
            return { 'S': val };
          else
          if (type=='boolean')
            return { 'N': (val ? 1 : 0)+'' };
          else
            throw new Error('unknown type of value: '+type);
      },
      _attr= function (o)
      {
          var obj= {};
          
          obj[o.attr]= _value(o.value);

          return obj; 
      },
      _error= function (err)
      {
          return _.extend(new Error(),err);
      },
      _item= function (Item)
      {
           var obj= {};

           Object.keys(Item).forEach(function (key)
           {
              if (key=='__jsogObjectId')
              { /*ignore*/ }
              else
              if (Item[key].S !== undefined)
                obj[key]= Item[key].S;
              else
              if (Item[key].N !== undefined)
              {
                 if (Item[key].N.indexOf('.')>-1)
                   obj[key]= parseFloat(Item[key].N);
                 else
                   obj[key]= parseInt(Item[key].N);
              }
              else
              if (Item[key].B !== undefined)
                obj[key]= new Buffer(Item[key].B,'base64');
              else
              if (Item[key].SS !== undefined)
                obj[key]= Item[key].SS;
              else
              if (Item[key].NS !== undefined)
                obj[key]= _.collect(Item[key].NS,function (n)
                {
                     if (n.indexOf('.')>-1)
                       return parseFloat(n);
                     else
                       return parseInt(n);
                });
              else
              if (Item[key].BS !== undefined)
                obj[key]= _.collect(Item[key].BS,function (b) { return new Buffer(b,'base64'); });
           });

           return obj;
      },
      _toItem= function (obj)
      {
           var Item= {};

           Object.keys(obj).forEach(function (key)
           {
                 if (key!='__jsogObjectId')
                   Item[key]= _value(obj[key]); 
           });

           return Item;
      },
      failedWritesQueue= async.queue(function (f,done)
      {
          setTimeout(function ()
          {
              console.log('retrying a failed write'.yellow);
              f();
              done(); 
          },300);
      },3),
      _failed= function (f)
      {
          console.log('Your provisioned write throughput has been exceeded'.yellow);
          failedWritesQueue.push(f);
      };

failedWritesQueue.saturated= function ()
{
    console.log('trying to keep up'.red);
}

failedWritesQueue.drain= function ()
{
    console.log('retrying failed writes'.green);
}
/*
 *
 */

module.exports= function (opts,tx)
{
    opts= _.defaults(opts || {},
    { 
         accessKeyId: process.env.AWS_ACCESS_KEY_ID, 
         secretAccessKey: process.env.AWS_SECRET_KEY || process.env.AWS_SECRET_ACCESS_KEY,
         region: process.env.AWS_REGION
    });

    AWS.config.update(opts);

    var _dyn = new AWS.DynamoDB(_.pick(opts,['endpoint'])), 
         dyn = { ctx: {}, queue: { error: [] } },
         _promise= function (success,error,info)
         {     
               success= success || [];
               error= error || [];
               info= info || [];
                  
               success= Array.isArray(success) ? success : [success];
               error= Array.isArray(error) ? error : [error];
               info= Array.isArray(info) ? info : [info];

               var promise= { queue: {}, trigger: {} },
                   _conf= function (also)
                          { 
                              return function (arg)
                              {
                                   promise.queue[arg]= [];

                                   promise[arg]= function (cb)
                                   {
                                       if (!(cb instanceof Function))
                                         throw new Error('Trying to bind a promise without a callback: '+cb);

                                       promise.queue[arg].push(cb);
                                       return promise;
                                   };

                                   promise.trigger[arg]= function ()
                                   {
                                        var args= arguments;

                                        promise.queue[arg].forEach(function (cb)
                                        {
                                           cb.apply(null,args);
                                        });

                                        also && also(arg);
                                   };
                              };
                          };

               _.union(['success','error'],info).forEach(_conf());

               success.forEach(_conf(promise.trigger.success));
               error.forEach(_conf(function (code) { promise.trigger.error(_error({ code: code }),true); }));

               promise.trigger.error= function (err,also)
               {
                    if (!also&&promise.trigger[err.code])
                        promise.trigger[err.code]();
                    else 
                        promise.queue.error.forEach(function (cb)
                        {
                           cb.apply(null,[err]);
                        });
               };

               promise.should= function (what)
               {
                   return function (err)
                   {
                       if (err)
                         promise.trigger.error(err);
                       else
                       {
                           var args= Array.prototype.slice.apply(arguments);
                           args.shift();

                           promise.trigger[what].apply(args);
                       } 
                   };
               };

               promise.chain= function (p)
               {
                  _.union(success,info).forEach(function (type) { var trg= p.trigger[type];  if (trg) promise[type](p.trigger[type]); });
                  promise.error(p.trigger.error);
               };

               return promise;
         },
         _results= function (opts,query,promise,op,cb)
         {
             return function _iterator(count)
             {
               op(query,
               function (err,data)
               {
                  if (data&&data.ConsumedCapacity)
                    promise.trigger.consumed({ table: query.TableName, read: data.ConsumedCapacity.CapacityUnits, write: 0 });

                  if (err)
                    promise.trigger.error(err);
                  else
                  {
                    if (opts.count)
                    {
                          if (data.LastEvaluatedKey)
                          {
                              query.ExclusiveStartKey= data.LastEvaluatedKey;
                              var cnt= data.Count+(count || 0);
                              promise.trigger.progress(cnt);
                              _iterator(cnt);
                          }
                          else
                              _catch(cb)(data.Count+(count || 0));
                    }
                    else
                    {     
                          var results= _.collect(data.Items,_item);

                          if (!!data.LastEvaluatedKey)
                            results.next= function ()
                            { 
                                query.ExclusiveStartKey= data.LastEvaluatedKey;
                                _iterator(); 
                            };

                          _catch(cb)(results);

                          if (!results.next)
                            promise.trigger.end(); 
                    }
                  }
               });
             };
         };

    dyn.error= function (fn)
    {
        dyn.queue.error.push(fn);
        return dyn;
    };

    dyn.table= function (name)
    {
        dyn.ctx.table= name;
        return dyn;
    };

    dyn.index= function (name)
    {
        dyn.ctx.index= name;
        return dyn;
    };

    dyn.hash= function (attr,value,operator)
    {
        var args= Array.prototype.slice.call(arguments);

        dyn.ctx.hash= { attr: attr, value: value, operator: operator || 'EQ' };

        return dyn;
    };

    dyn.range= function (attr,value,operator)
    {
        dyn.ctx.range= { attr: attr, value: value, operator: operator || 'EQ' };
        return dyn;
    };

    dyn.get= function (cb,opts)
    {
       opts= opts || {};

       var query= { TableName: dyn.ctx.table };

       query.Key= {};

       _.extend(query.Key,_attr(dyn.ctx.hash));

       if (dyn.ctx.range)
         _.extend(query.Key,_attr(dyn.ctx.range));

       if (opts.attrs)
         query.AttributesToGet= opts.attrs;

       if (opts.consistent)
         query= _.extend(query,{ ConsistentRead: true });

       if (dyn.iscli())
         query= _.extend(query,{ ReturnConsumedCapacity: 'TOTAL' });
         

       var promise= _promise(null,'notfound','consumed');

       process.nextTick(function ()
       {
           if (debug) console.log('get',JSON.stringify(query,null,2),opts);

           _dyn.getItem(query,
           function (err,data)
           {
                  if (data&&data.ConsumedCapacity)
                    promise.trigger.consumed({ table: query.TableName, read: data.ConsumedCapacity.CapacityUnits, write: 0 });
                    
                  if (err)
                    promise.trigger.error(err);
                  else
                  if (!data.Item)
                    promise.trigger.notfound();
                  else
                    _catch(cb)(_item(data.Item));
           });
       });

       dyn.ctx= {};

       return promise; 
    };

    dyn.query= function (cb,opts)
    {
       opts= opts || {};

       var query= { TableName: dyn.ctx.table };

       query.KeyConditions= {}; 

       query.KeyConditions[dyn.ctx.hash.attr]= { AttributeValueList: _.collect(_arr(dyn.ctx.hash.value),_value),
                                                 ComparisonOperator: dyn.ctx.hash.operator };

       if (dyn.ctx.range)
         query.KeyConditions[dyn.ctx.range.attr]= { AttributeValueList: _.collect(_arr(dyn.ctx.range.value),_value),
                                                    ComparisonOperator: dyn.ctx.range.operator };

       if (dyn.ctx.index)
         query.IndexName= dyn.ctx.index; 

       if (opts.attrs)
         query.AttributesToGet= opts.attrs;

       if (opts.consistent)
         query= _.extend(query,{ ConsistentRead: true });

       if (opts.desc)
         query= _.extend(query,{ ScanIndexForward: false });

       if (opts.limit)
         query= _.extend(query,{ Limit: opts.limit });

       if (opts.count)
       {
         query= _.extend(query,{ Select: 'COUNT' });
         delete query.AttributesToGet;
       }

       if (dyn.iscli())
         query= _.extend(query,{ ReturnConsumedCapacity: 'TOTAL' });

       var promise= _promise('end',null,['progress','consumed']);

       if (debug) console.log('query',JSON.stringify(query,null,2),opts);

       process.nextTick(_results(opts,query,promise,_.bind(_dyn.query,_dyn),cb));

       dyn.ctx= {};
 
       return promise; 
    };

    dyn.put= function (obj,cb,opts)
    {
       opts= opts || {};

       var query= { TableName: dyn.ctx.table, Item: _toItem(obj) };

       if (opts.exists)
       {
           query.Expected= {}

           query.Expected[dyn.ctx.hash.attr]= { Exists: true, Value: _value(obj[dyn.ctx.hash.attr]) };

           if (dyn.ctx.range)
             query.Expected[dyn.ctx.range.attr]= { Exists: true, Value: _value(obj[dyn.ctx.range.attr]) };
       }
       else
       if (opts.exists===false)
       {
           query.Expected= {}

           query.Expected[dyn.ctx.hash.attr]= { Exists: false };

           if (dyn.ctx.range)
             query.Expected[dyn.ctx.range.attr]= { Exists: false };
       } 

       if (opts.expected)
       {
           query.Expected= query.Expected || {};

           _.keys(opts.expected).forEach(function (attr)
           {
               var exp= opts.expected[attr];
               query.Expected[attr]= { Exists: !!exp };
               
               if (!!exp)
                 query.Expected[attr].Value= _value(exp);
           });
       }

       if (dyn.iscli())
         query= _.extend(query,{ ReturnConsumedCapacity: 'TOTAL' });

       var promise= _promise(null,['found','notfound'],'consumed');

       process.nextTick(function putter()
       {

               if (debug) console.log('put',JSON.stringify(query,null,2),opts);

               _dyn.putItem(query,
               function (err,data)
               {
                 if (debug) console.log('put',err,data);

                 if (data&&data.ConsumedCapacity)
                   promise.trigger.consumed({ table: query.TableName, read: 0, write: data.ConsumedCapacity.CapacityUnits });

                 if (err)
                 {
                    if (err.code=='ProvisionedThroughputExceededException')
                    {
                        _failed(putter);   
                        return;
                    }
                    
                    if (err.code=='ConditionalCheckFailedException')
                    {
                        if (opts.exists||opts.expected)
                          promise.trigger.notfound();
                        else
                          promise.trigger.found();
                    }
                    else
                      promise.trigger.error(err);
                 }
                 else
                    _catch(cb)();
               }); 
       });

       dyn.ctx= {};
 
       return promise; 
    };

    dyn.updateItem= function (opts,cb)
    {
       opts= opts || {};

       var query= { TableName: dyn.ctx.table, AttributeUpdates: {} };

       query.Key= {}; 

       query.Key[dyn.ctx.hash.attr]= _value(dyn.ctx.hash.value);

       if (dyn.ctx.range)
         query.Key[dyn.ctx.range.attr]= _value(dyn.ctx.range.value);

       if (opts.expected)
       {
           query.Expected= {};

           _.keys(opts.expected).forEach(function (attr)
           {
               var exp= opts.expected[attr];
               query.Expected[attr]= { Exists: !!exp, Value: _value(exp) };
           });
       }

       if (opts.returning)
         query.ReturnValues= opts.returning;

       _.keys(opts.update).forEach(function (attr)
       {
           var upd= opts.update[attr];
           query.AttributeUpdates[attr]= { Action: upd.action, Value: upd.value!==undefined ? _value(upd.value) : null };
       });

       if (dyn.iscli())
         query= _.extend(query,{ ReturnConsumedCapacity: 'TOTAL' });

       var promise= _promise([],'notfound','consumed');

       process.nextTick(function putter()
       {
               if (debug) console.log('updateItem',JSON.stringify(query,null,2),opts);

               _dyn.updateItem(query,
               function (err,data)
               {
                 if (data&&data.ConsumedCapacity)
                   promise.trigger.consumed({ table: query.TableName, read: 0, write: data.ConsumedCapacity.CapacityUnits });

                 if (err)
                 {
                    if (err.code=='ProvisionedThroughputExceededException')
                    {
                        _failed(putter);   
                        return;
                    }
                    
                    if (err.code=='ConditionalCheckFailedException')
                      promise.trigger.notfound();
                    else
                      promise.trigger.error(err);
                 }
                 else
                    _catch(cb)(data);
               }); 
       });

       dyn.ctx= {};
 
       return promise; 
    };

    dyn.mput= function (ops,cb)
    {
       var query= { RequestItems: {} },
           _operation= function (op)
           {
              var r= {};

              if (op.op=='put')
                r.PutRequest= { Item: _toItem(op.item) };
              else
              if (op.op=='del')
                r.DeleteRequest= { Key: _toItem(op.item) };
              else
                throw new Error('Unknown op type: '+op.op); 

              return r;
           };

       Object.keys(ops).forEach(function (table)
       {
            query.RequestItems[table]= _.collect(ops[table],_operation);
       });

    //   console.log(JSON.stringify(query,null,2));

       var promise= _promise();

       process.nextTick(function ()
       {
           _dyn.batchWriteItem(query,
           function (err,data)
           {       
                    var retry= function (UnprocessedItems)
                    {
                        return function ()
                        {
                            _dyn.batchWriteItem({ RequestItems: UnprocessedItems },
                            function (err, data)
                            {
                                if (err)
                                {
                                  if (err.code=='ProvisionedThroughputExceededException')
                                    _failed(retry(UnprocessedItems));
                                  else
                                    promise.trigger.error(err);
                                }
                                else
                                if (_.keys(data.UnprocessedItems).length)
                                  _failed(retry(data.UnprocessedItems));
                                else
                                  _catch(cb)();
                            });
                        }
                    };

                  if (err)
                    promise.trigger.error(err);
                  else
                  if (_.keys(data.UnprocessedItems).length)
                    _failed(retry(data.UnprocessedItems));
                  else
                    _catch(cb)();
           });
       });

       dyn.ctx= {};
 
       return promise; 
    };

    dyn.count= function (cb,opts)
    {
       opts= opts || {};

       var query= { TableName: dyn.ctx.table, Select: 'COUNT' };

       query.KeyConditions= {}; 

       query.KeyConditions[dyn.ctx.hash.attr]= { AttributeValueList: _.collect(_arr(dyn.ctx.hash.value),_value),
                                                 ComparisonOperator: dyn.ctx.hash.operator };

       if (dyn.ctx.range)
         query.KeyConditions[dyn.ctx.range.attr]= { AttributeValueList: _.collect(_arr(dyn.ctx.range.value),_value),
                                                    ComparisonOperator: dyn.ctx.range.operator };

       if (dyn.ctx.index)
         query.IndexName= dyn.ctx.index; 

       if (opts.consistent)
         query= _.extend(query,{ ConsistentRead: true });

       var promise= _promise();

       process.nextTick(function ()
       {
           _dyn.query(query,
           function (err,data)
           {
                  if (err)
                    promise.trigger.error(err);
                  else
                    cb(data.Count);
           });
       });

       dyn.ctx= {};
 
       return promise; 
    };

    dyn.create= function (cb,opts)
    {
       opts= opts || {};
       opts.throughput= opts.throughput || {};

       var query= { 
                    AttributeDefinitions: [],
                    KeySchema: [],
                    ProvisionedThroughput: { ReadCapacityUnits:  opts.throughput.read || 1,
                                             WriteCapacityUnits: opts.throughput.write || 1 },
                    TableName: dyn.ctx.table
                  };

       query.AttributeDefinitions.push({ AttributeName: dyn.ctx.hash.attr,
                                         AttributeType: dyn.ctx.hash.value });

       query.KeySchema.push({ AttributeName: dyn.ctx.hash.attr,
                              KeyType: 'HASH' });

       
       if (dyn.ctx.range)
       {
         query.AttributeDefinitions.push({ AttributeName: dyn.ctx.range.attr,
                                           AttributeType: dyn.ctx.range.value });

         query.KeySchema.push({ AttributeName: dyn.ctx.range.attr,
                                KeyType: 'RANGE' });
       }

       if (opts.secondary&&opts.secondary.length>0)
         query.LocalSecondaryIndexes= _.collect(opts.secondary,
                                      function (idx) 
                                      { 
                                        var _idx= { 
                                                  IndexName: idx.name,
                                                  KeySchema: [_.findWhere(query.KeySchema,{KeyType: 'HASH'}),
                                                              { AttributeName: idx.key.name, KeyType: 'RANGE' }] 
                                               }; 

                                        query.AttributeDefinitions.push({ AttributeName: idx.key.name,
                                                                          AttributeType: idx.key.type });

                                        if (!idx.projection)
                                          _idx.Projection= { ProjectionType: 'ALL' };
                                        else
                                          _idx.Projection= { NonKeyAttributes: idx.projection, ProjectionType: 'INCLUDE' };

                                        return _idx;
                                      });

       var promise= _promise();

       process.nextTick(function ()
       {
           _dyn.createTable(query,
           function (err)
           {
              if (err)
                promise.trigger.error(err);
              else
                _catch(cb)();
           });
       });

       dyn.ctx= {};
 
       return promise; 
    };

    dyn.scan= function (cb,opts)
    {
       opts= opts || {};

       var query= { TableName: dyn.ctx.table },
           _filterField= function (field)
           {
               return { AttributeValueList: _.collect(field.values,_value),
                        ComparisonOperator: field.op };
           },
           _filter= function (filter)
           {
               var r= {};

               Object.keys(filter).forEach(function (field)
               {
                   r[field]= _filterField(filter[field]);
               });

               return r;
           };

       if (opts.attrs)
         query.AttributesToGet= opts.attrs;

       if (opts.limit)
         query= _.extend(query,{ Limit: opts.limit });

       if (opts.filter)
         query= _.extend(query,{ ScanFilter: _filter(opts.filter) });

       if (opts.count)
         query= _.extend(query,{ Select: 'COUNT' });

       if (opts.segment)
         query= _.extend(query,{ Segment: opts.segment.no, TotalSegments: opts.segment.of });

       if (dyn.iscli())
         query= _.extend(query,{ ReturnConsumedCapacity: 'TOTAL' });

    //   console.log(JSON.stringify(query,null,2));

       var promise= _promise('end',null,['progress','consumed']);

       process.nextTick(_results(opts,query,promise,_.bind(_dyn.scan,_dyn),cb));

       dyn.ctx= {};
 
       return promise; 
    };

    dyn.delete= function (cb,opts)
    {
       opts= opts || {};

       var query= { TableName: dyn.ctx.table };

       query.Key= {};

       _.extend(query.Key,_attr(dyn.ctx.hash));

       if (dyn.ctx.range)
         _.extend(query.Key,_attr(dyn.ctx.range));

       if (opts.exists===true)
       {
           query.Expected= {}

           query.Expected[dyn.ctx.hash.attr]= { Exists: true, Value: _value(dyn.ctx.hash.value) };

           if (dyn.ctx.range)
             query.Expected[dyn.ctx.range.attr]= { Exists: true, Value: _value(dyn.ctx.range.value) };
       }

       if (dyn.iscli())
         query= _.extend(query,{ ReturnConsumedCapacity: 'TOTAL' });

       var promise= _promise(null,'notfound','consumed');

       process.nextTick(function deleter()
       {
               if (debug) console.log('delete',JSON.stringify(query,null,2),opts);

               _dyn.deleteItem(query,
               function (err,data)
               {
                 if (data&&data.ConsumedCapacity)
                   promise.trigger.consumed({ table: query.TableName, read: 0, write: data.ConsumedCapacity.CapacityUnits });

                 if (err)
                 {
                    if (err.code=='ProvisionedThroughputExceededException')
                    {
                        _failed(deleter);
                        return;
                    }
                    
                    if (err.code=='ConditionalCheckFailedException')
                      promise.trigger.notfound();
                    else
                      promise.trigger.error(err);
                 }
                 else
                    _catch(cb)();
               }); 
       });

       dyn.ctx= {};
 
       return promise; 
    };

    dyn.listTables= function (cb)
    {
        _dyn.listTables(function (err,data)
        {
            _catch(cb)(err,err ? null : data.TableNames);
        });
    };

    dyn.describeTable= function (table,cb)
    {
        _dyn.describeTable({ TableName: table },function (err,data)
        {
            _catch(cb)(err,err ? null : data);
        });
    };

    dyn.deleteTable= function (table,cb)
    {
        _dyn.deleteTable({ TableName: table },function (err,data)
        {
            _catch(cb)(err,err ? null : data);
        });
    };

    dyn.updateTable= function (table,read,write,cb)
    {
        _dyn.updateTable({ TableName: table,
                           ProvisionedThroughput: { ReadCapacityUnits: read,
                                                    WriteCapacityUnits: write } },
        function (err,data)
        {
            _catch(cb)(err,err ? null : data);
        });
    };

    dyn.iscli= function ()
    {
       return 'dyngodb2'==argv.$0;
    }

    dyn.promise= _promise;

    dyn.stream= function (table)
    {
        var methods= {},
            _readable= function (rstream)
            {
                 var emit= { 
                             data: function (items)
                                   {
                                      if (items.length) rstream.emit('data',items);
                                      
                                      if (items.next) 
                                      {
                                          if (!rstream.paused)
                                            items.next();
                                          else
                                            _resume= _.partial(items.next);
                                      }
                                      else
                                          _resume= undefined;
                                   },
                             error:  _.bind(rstream.emit,rstream,'error'),
                             end:  _.bind(rstream.emit,rstream,'end')
                           },
                     _resume;

                 rstream.readable= true;
                 rstream.paused= false;

                 rstream.pause= function ()
                 {
                   rstream.paused= true;
                 };

                 rstream.resume= function ()
                 {
                   rstream.paused= false;
                   _resume && _resume();
                 };

                 rstream.pipe= function (wstream)
                 {
                    wstream.on('drain',function () { rstream.resume(); });

                    rstream
                        .on('data',function (items)
                        {
                          if (!wstream.write(items))
                            rstream.pause();
                        })
                        .on('end',wstream.end);

                    return wstream;
                 };

                 rstream.remit= emit;
                 
                 return rstream;
            };

        methods.readable= function (read)
        {
             var rstream= _readable(new Stream());
             read(rstream.remit);
             return rstream;
        };

        methods.writable= function (write)
        {
             var wstream= new Stream(),
                 emit= { 
                         drain: _.bind(wstream.emit,wstream,'drain'),
                         error:  _.bind(wstream.emit,wstream,'error'),
                         finish:  _.bind(wstream.emit,wstream,'finish')
                       },
                 _ops= function (items)
                 {
                    var ops= {};
                    ops[table]= _.collect(items,function (item) { return { op: op, item: item }; });
                    return ops;
                 };

             wstream.writeable= true;

             wstream.write= function (items)
             {
                 if (items&&items.length)
                   write(items,{ done: emit.drain, error: emit.error });
                 else
                   emit.drain();

                 return false;
             };

             wstream.end= function (items)
             {
                 if (items&&items.length)
                   write(items,{ done: emit.finish, error: emit.error },true);
                 else
                   emit.finish();
             };

             wstream.wemit= emit;

             return wstream;
        };

        methods.transform= function (transform)
        {
             var wstream= methods.writable(function (items,emit,end)
                          {
                                if (transform.async)
                                  transform(items, function (err, transformed)
                                  {
                                     if (err)
                                       wstream.emit('error',err); 
                                     else
                                       wstream.emit(end ? 'end' : 'data',transformed); 
                                  });
                                else
                                  wstream.emit(end ? 'end' : 'data',transform(items)); 

                                if (end)
                                  emit.done();
                                else
                                  wstream.resume= emit.done;
                          });

             wstream.end= function (items)
             {
                 var _finish= function ()
                     {
                         wstream.wemit.finish();
                         wstream.emit('end');
                     };

                 if (items&&items.length)
                   write(items,{ done: wstream.wemit.finish, error: emit.error },true);
                 else
                   _finish();
             };

             _readable(wstream);

             return wstream;
        };

        methods.scan= function (opts)
        {
             return methods.readable(function (emit)
             {
                 dyn.table(table)
                    .scan(emit.data,opts)
                    .error(emit.error)
                    .end(emit.end);
             });
        };

        methods.mput= function (op)
        {
             var ops= {
                         put: function (item,done)
                         {
                               dyn.table(table)
                                  .put(item,done)
                                  .error(done);
                         },
                         del: function (item,done)
                         {
                               var cmd= dyn.table(table);

                               if (item._hash) // index
                                  cmd= cmd.hash('_hash',item._hash)
                                          .range('_range',item._range);
                               else // table
                                  cmd= cmd.hash('_id',item._id)
                                          .range('_pos',item._range);

                               cmd.delete(done)
                                  .error(done);
                         }
                      };

             return methods.writable(function (items,emit)
             {
                   async.forEach(items,
                   ops[op],
                   function (err)
                   {
                       if (err)
                         emit.error(err);
                       else
                         emit.done();
                   });
             });

        };

        return methods;
    };

    dyn.syncResults= function (endCb)
    {
        var finished= _.after(2,_.once(endCb)), wk= 0, fetching= false;

        return {
                  results: function (itemsCb)
                  {
                     return function (items)
                     {
                         fetching= false;
                         wk++;

                         if (items.next)
                           items.next= _.wrap(items.next,
                                       function (wrapped)
                                       {
                                          fetching= true;
                                          wrapped();
                                       });

                         process.nextTick(function () // execute after end
                         {
                             itemsCb(items,function (err)
                             {
                                 if ((--wk==0&&!fetching)||err)
                                   finished(err); // the first error gets called the other ignored _.once above
                             });
                         });
                     };
                  },
                  end: finished
               }
    };

    dyn.ref= function (item)
    {
       return [item._id,item._pos,item._table].join('$:$');
    };

    dyn.deref= function (ref,table)
    {
       if (typeof ref=='string')
       {
           var parts= ref.split('$:$'); 

           return { _id: parts[0], _pos: +parts[1] || 0, _table: parts[2] || table };
       }
       else
           return _.defaults(ref,{ _pos: 0, _table: table });
    };

    if (tx._id)
      configureTransaction(dyn,tx);

    return dyn;
}
