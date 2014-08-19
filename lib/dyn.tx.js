var _= require('underscore'),
    debug = require('optimist').argv['dyn-debug'],
    async= require('async');

const BECONSISTENT= { consistent: true },
      GETSTATE= _.extend({ attrs: ['state'] },BECONSISTENT);

module.exports= function configureTransaction(dyn,tx)
{
     dyn.putNT= dyn.put;
     dyn.updateItemNT= dyn.updateItem;
     dyn.deleteNT= dyn.delete;
     dyn.getNT= dyn.get;
     dyn.queryNT= dyn.query;
     dyn.scanNT= dyn.scan;

     var _ctxId= function (ctx,item)
         {
            if (!ctx.hash)
            {
               if (item._hash!==undefined)
                 ctx.hash= { attr: '_hash', value: item._hash };
               else
               if (item._id!==undefined)
                 ctx.hash= { attr: '_id', value: item._id };
               else
                 console.log('dyn.tx.js: unknown hash'.red,item); // :(
            }
           
            if (!ctx.range)
            {
               if (item._range!==undefined)
                 ctx.range= { attr: '_range', value: item._range };
               else
               if (item._pos!==undefined)
                 ctx.range= { attr: '_pos', value: item._pos };
               else
                 console.log('dyn.tx.js: unknown range'.red,item); // :(
            }

            var _id= function (prefix)
                     {
                         return [prefix,
                                 ctx.table,
                                 ctx.hash.attr,
                                 ctx.hash.value,
                                 ctx.range.attr,
                                 ctx.range.value].join('::');
                     };

            _id.ctx= ctx;

            return _id;
         }, 
         _perform= function (dynI,ctx,p,obj,op,_apply)
         {
             var _id= _ctxId(ctx),
                 txR= function ()
                      {
                          return dyn.table(tx.txTable._dynamo.TableName)
                                    .hash('_id',tx._id)
                                    .range('_item','_');
                      },
                 dynT= function ()
                       {
                          return dyn.table(tx.txTable._dynamo.TableName)
                                    .hash('_id',tx._id)
                                    .range('_item',_id('target'));
                       },
                 dynC= function ()
                       {
                          return dyn.table(tx.txTable._dynamo.TableName)
                                    .hash('_id',tx._id)
                                    .range('_item',_id('copy'));
                       },
                 _decide= function (txId,cb)
                 {
                     tx.transaction(txId)
                       .transaction(function (competing)
                     {
                         if (competing.state=='pending')
                           competing.rollback().rolledback(cb).error(p.trigger.error).consumed(p.trigger.consumed);
                         else
                           competing.commit().committed(cb).error(p.trigger.error).consumed(p.trigger.consumed);
                     })
                     .error(function (err)
                     {
                          if (err.code=='notfound')
                            dynI()
                               .updateItemNT({ update: { _txTransient: { action: 'DELETE' },
                                                         _txApplied: { action: 'DELETE' },
                                                         _txDeleted: { action: 'DELETE' },
                                                         _txLocked: { action: 'DELETE' },
                                                         _tx: { action: 'DELETE' } } },
                                cb)
                               .chain(p);
                          else
                            p.trigger.error(err);
                     })
                     .consumed(p.trigger.consumed);
                 },
                 _lock= function (p,_locked)
                 {
                     dynI().getNT(function (item)
                     {
                         if (!item._tx) // item lock free
                         {
                            item._tx= tx._id;
                            item._txLocked= new Date().toISOString();

                            // set lock
                            dynI().putNT(item,function ()
                            {
                               _locked(item);
                            },{ exists: true, expected: { _tx: false } })
                            .consumed(p.trigger.consumed)
                            .error(p.trigger.error);
                         }
                         else // the item is locked
                         {
                             if (item._txTransient)
                               obj['_txTransient']= true;

                             if (item._tx==tx._id) // ok: lock already acquired
                               _locked(item);
                             else
                             if (item._tx!=tx._id) // mm: lock acquired by another transaction
                               _decide(item._tx,function ()
                               {
                                   _lock(p,_locked);
                               });
                         }
                     },BECONSISTENT)
                     .consumed(p.trigger.consumed)
                     .error(function (err)
                     {
                        if (err.code=='notfound'&&op=='put') // insert a transient item to acquire the lock
                        {
                           var item= { _tx: tx._id };
                           item[ctx.hash.attr]= ctx.hash.value;
                           item[ctx.range.attr]= ctx.range.value;
                           item['_txTransient']= true;
                           obj['_txTransient']= true;

                           dynI().putNT(item,function ()
                           {
                             _locked(item,true);
                           },{ exists: false })
                           .consumed(p.trigger.consumed)
                           .error(function ()
                           {
                              if (err.code=='found')
                                p.trigger.error(new Error('Someone inserted an item while we were trying to acquire a lock'));
                              else
                                p.trigger.error(err);
                           });
                        }
                        else
                          p.trigger.error(err);
                     });

                 },
                 _save= function (p,item,cb)
                 {
                     var t= dynC();
                     t.putNT(_.extend({},item,{ _id: dyn.ctx.hash.value, _item: dyn.ctx.range.value }),cb,{ exists: false })
                     .consumed(p.trigger.consumed)
                     .error(function (err)
                     {
                          if (err.code!='found')
                            p.trigger.error(err);
                          else
                            cb();
                     });
                 },
                 _add= function (p,cb)
                 {
                     txR().getNT(function (_txR)
                     {
                       if (_txR.state=='pending')
                       {
                         var t= dynT();

                         t.putNT(_.extend({ _txOp: op },{ update: JSON.stringify(obj) },{ _id: dyn.ctx.hash.value, _item: dyn.ctx.range.value }),cb)
                         .consumed(p.trigger.consumed)
                         .error(p.trigger.error);
                       }
                       else
                         p.trigger.rolledback(true);
                     },GETSTATE)
                     .consumed(p.trigger.consumed)
                     .error(p.trigger.error);
                 },
                 _verify= function (p,cb)
                 {
                     txR().getNT(function (_txR)
                     {
                       if (_txR.state=='pending')
                         cb();
                       else
                         p.trigger.rolledback(true);
                     },GETSTATE)
                     .consumed(p.trigger.consumed)
                     .error(p.trigger.error);
                 };


            // @see https://github.com/awslabs/dynamodb-transactions/blob/master/DESIGN.md#no-contention
            _add(p,function ()
            {
                  _lock(p,function (item,skipSave)
                  {
                      if (skipSave||op=='get')
                        _verify(p,function ()
                        {
                            _apply(item);
                        });
                      else
                        _save(p,item,function ()
                        {
                            _verify(p,_apply);
                        });
                  });
            });
         },
         _selectTxAttrs= function (opts)
         {
             opts.consistent= true;

             if (opts.attrs)
             {

                 if (!_.contains(opts.attrs,'_tx'))
                   opts.attrs.push('_tx');

                 if (!_.contains(opts.attrs,'_txApplied'))
                   opts.attrs.push('_txApplied');

                 if (!_.contains(opts.attrs,'_txDeleted'))
                   opts.attrs.push('_txDeleted');

                 if (!_.contains(opts.attrs,'_txTransient'))
                   opts.attrs.push('_txTransient');

             }
         },
         _getTxItem= function (item,_id,p,cb)
         {
             if (item._tx)
             {
                  if (tx._id!=item._tx)
                  {
                       if (item._txTransient)
                         cb(null);
                       else
                       if (item._txApplied)
                       {
                           var ctx= _id.ctx;

                           dyn.table(tx.txTable._dynamo.TableName)
                              .hash('_id',item._tx)
                              .range('_item',_id('copy'))
                              .get(function (copy)
                           {
                              delete copy['_id'];
                              delete copy['_item'];

                              copy[ctx.hash.attr]= ctx.hash.value;
                              copy[ctx.range.attr]= ctx.range.value;

                              cb(null,copy);
                           })
                           .consumed(p.trigger.consumed)
                           .error(cb);
                       }
                       else
                           cb(null,item);
                  }
                  else
                  if (item._txDeleted)
                    cb(null);
                  else
                    cb(null,item);
             }
             else
               cb(null,item);
         },
         _results= function (fn)
         {
              return function (cb,opts)
              {
                 opts= opts || {};

                 var p= dyn.promise('end',null,['progress','consumed']),
                     ctx= JSON.parse(JSON.stringify(dyn.ctx)),
                     sync= dyn.syncResults(function (err)
                     {
                        if (err)
                          p.trigger.error(err);
                        else
                          p.trigger.end();
                     });

                 _selectTxAttrs(opts);

                 fn(sync.results(function (items,done)
                 {
                        async.forEach(_.keys(items),
                        function (key,done)
                        {
                           var idx= +key, item= items[idx];

                           _getTxItem(item,_ctxId(ctx,item),p,function (err,item)
                           {
                              if (err)
                                done(err);
                              else
                              {
                                items[idx]= item;
                                done();
                              }
                           }); 
                        },
                        function (err)
                        {
                           if (err)
                             done(err);
                           else
                           {
                             cb(_.extend(_.filter(items,function (i) { return !!i; }),{ next: items.next }));
                             done();
                           }
                        });
                 }),opts)
                 .consumed(p.trigger.consumed)
                 .error(p.trigger.error)
                 .end(sync.end);

                 return p;
              };
         };

  dyn.put= function (obj,cb,opts)
  {
     var p= dyn.promise(null,['found','notfound','rolledback'],'consumed'),
         ctx= JSON.parse(JSON.stringify(dyn.ctx)),
         dynI= function ()
               {
                  return dyn.table(ctx.table)
                            .hash(ctx.hash.attr,ctx.hash.value)
                            .range(ctx.range.attr,ctx.range.value);
               };

     _perform(dynI,ctx,p,obj,'put',function ()
     {
        obj['_txApplied']= true;
        obj['_tx']= tx._id;
        opts.expected= opts.expected || {};
        opts.expected['_tx']= tx._id;

        dynI()
          .putNT(obj,cb,opts)
          .chain(p);
     });

     return p;
  };

  dyn.updateItem= function (opts,cb)
  {
     var p= dyn.promise(null,['rolledback'],'consumed'),
         ctx= JSON.parse(JSON.stringify(dyn.ctx)),
         dynI= function ()
               {
                  return dyn.table(ctx.table)
                            .hash(ctx.hash.attr,ctx.hash.value)
                            .range(ctx.range.attr,ctx.range.value);
               };

     _perform(dynI,ctx,p,opts.update,'updateItem',function ()
     {
        opts.update['_txApplied']= { action: 'PUT', value: true };
        opts.expected= opts.expected || {};
        opts.expected['_tx']= tx._id;

        dynI()
          .updateItemNT(opts,cb)
          .chain(p);
     });

     return p;
  };

  dyn.delete= function (cb,opts)
  {
     var p= dyn.promise(null,['found','notfound','rolledback'],'consumed'),
         ctx= JSON.parse(JSON.stringify(dyn.ctx)),
         dynI= function ()
               {
                  return dyn.table(ctx.table)
                            .hash(ctx.hash.attr,ctx.hash.value)
                            .range(ctx.range.attr,ctx.range.value);
               };

     _perform(dynI,ctx,p,{},'delete',function ()
     {
        opts= opts || {};
        opts.expected= opts.expected || {};
        opts.expected['_tx']= tx._id;

        dynI()
          .updateItemNT({ update: { _txDeleted: { action: 'PUT', value: true } } },
                        function () { cb(); })
          .chain(p);
     });

     return p;
  };

  dyn.get= function (cb,opts)
  {
     opts= opts || {};

     var p= dyn.promise(null,['found','notfound','rolledback'],'consumed'),
         ctx= JSON.parse(JSON.stringify(dyn.ctx)),
         _id= _ctxId(ctx),
         dynI= function ()
               {
                  return dyn.table(ctx.table)
                            .hash(ctx.hash.attr,ctx.hash.value)
                            .range(ctx.range.attr,ctx.range.value);
               };

     _selectTxAttrs(opts);

     if (opts.lock)
       _perform(dynI,ctx,p,{},'get',function (item)
       {
             cb(item);
       });
     else
       dynI()
          .getNT(function (item)
          {
             _getTxItem(item,_id,p,function (err,item)
             {
                if (err)
                  p.trigger.error(err);
                else
                if (item==undefined)
                  p.trigger.notfound();
                else
                  cb(item);
             });
          },
          BECONSISTENT)
          .chain(p);

     return p;
  };

  dyn.query= _results(_.bind(dyn.queryNT,dyn));

  dyn.scan= _results(_.bind(dyn.scanNT,dyn));

};
