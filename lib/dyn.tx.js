var _= require('underscore'),
    debug = require('optimist').argv['dyn-debug'],
    async= require('async');

const BECONSISTENT= { consistent: true },
      GETSTATE= _.extend({ attrs: ['state'] },BECONSISTENT);

module.exports= function configureTransaction(dyn,tx)
{
     dyn.putNT= dyn.put;
     dyn.deleteNT= dyn.delete;
     dyn.getNT= dyn.get;
     dyn.queryNT= dyn.query;
     dyn.scanNT= dyn.scan;

     var _perform= function (dynI,ctx,p,obj,op,_apply)
         {
             var _id= function (prefix)
                      {
                         return [prefix,
                                 ctx.table,
                                 ctx.hash.attr,
                                 ctx.hash.value,
                                 ctx.range.attr,
                                 ctx.range.value].join('::');
                      },
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
                           competing.rollack().rolledback(cb).chain(p);
                         else
                           competing.commit().committed(cb).chain(p);
                     })
                     .error(function (err)
                     {
                          if (err.code=='notfound')
                            dynI()
                               .updateItem({ update: { _txTransient: { action: 'DELETE' },
                                                       _txApplied: { action: 'DELETE' },
                                                       _tx: { action: 'DELETE' } } },
                                cb)
                               .chain(p);
                          else
                            p.trigger.error(err);
                     });
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
                     t.putNT(_.extend({},item,{ _id: dyn.ctx.hash.value, _item: dyn.ctx.range.value }),cb)
                     .consumed(p.trigger.consumed)
                     .error(p.trigger.error);
                 },
                 _add= function (p,cb)
                 {
                     txR().getNT(function (_txR)
                     {
                       if (_txR.state=='pending')
                       {
                         var t= dynT();
                         t.putNT(_.extend({ _txOp: op },obj,{ _id: dyn.ctx.hash.value, _item: dyn.ctx.range.value }),cb)
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
          .updateItem({ update: { _txDeleted: { action: 'PUT', value: true } } },
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
         _id= function (prefix)
              {
                 return [prefix,
                         ctx.table,
                         ctx.hash.attr,
                         ctx.hash.value,
                         ctx.range.attr,
                         ctx.range.value].join('::');
              },
         dynI= function ()
               {
                  return dyn.table(ctx.table)
                            .hash(ctx.hash.attr,ctx.hash.value)
                            .range(ctx.range.attr,ctx.range.value);
               };

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

     if (opts.lock)
       _perform(dynI,ctx,p,{},'get',function (item)
       {
             cb(item);
       });
     else
        dynI()
          .getNT(function (item)
          {
             if (item._tx)
             {
                  if (tx._id!=item._tx)
                  {
                       if (item._txTransient)
                         p.trigger.notfound();
                       else
                       if (item._txApplied)
                           dyn.table(tx.txTable._dynamo.TableName)
                              .hash('_id',item._tx)
                              .range('_item',_id('copy'))
                              .get(function (copy)
                           {
                              delete copy['_id'];
                              delete copy['_item'];

                              copy[ctx.hash.attr]= ctx.hash.value;
                              copy[ctx.range.attr]= ctx.range.value;

                              cb(copy);
                           })
                           .chain(p);
                       else
                           cb(item);
                  }
                  else
                  if (item._txDeleted)
                    p.trigger.notfound();
                  else
                    cb(item);
             }
             else
               cb(item);
          },
          BECONSISTENT)
          .chain(p);

     return p;
  };

  dyn.queryTx= function (cb,opts) // @TODO
  {
     opts= opts || {};

     var p= _promise('end',null,['progress','consumed']),
         ctx= JSON.parse(JSON.stringify(dyn.ctx));

     if (opts.attrs)
     {

         if (!_.contains(opts.attrs,'_tx'))
           opts.attrs.push('_tx');

         if (!_.contains(opts.attrs,'_txApplied'))
           opts.attrs.push('_txApplied');

         if (!_.contains(opts.attrs,'_txTransient'))
           opts.attrs.push('_txTransient');

     }

     dyn.queryNT(function (items)
         {
            async.forEach(_.keys(items),
            function (key,done)
            {
              var idx= +key, item= items[idx];

              if (item._tx&&tx._id!=item._tx&&item._txApplied&&!item._txTransient)
               dyn.table(tx.txTable._dynamo.TableName)
                  .hash('_id',item._tx)
                  .range('_item',_id('copy'))
                  .get(function (copy)
               {
                  delete copy['_id'];
                  delete copy['_item'];

                  copy[ctx.hash.attr]= ctx.hash.value;
                  copy[ctx.range.attr]= ctx.range.value;

                  items[idx]= copy;
                  done();
               })
               .consumed(p.trigger.consumed)
               .error(p.trigger.error);
              else
               done();
            },
            function (err)
            {
               cb(_.filter(items,function (i) { return !i._txTransient; }));
            });
         },opts)
        .consumed(p.trigger.consumed)
        .error(p.trigger.error);

     return p;
  };

  dyn.scanTx= function (cb,opts) // @TODO
  {
     opts= opts || {};

     var p= _promise('end',null,['progress','consumed']),
         ctx= JSON.parse(JSON.stringify(dyn.ctx));

     if (opts.attrs&&!_.contains(opts.attrs,'_tx'))
       opts.attrs.push('_tx');

     dyn.scanNT(function (items)
     {
        async.forEach(items,
        function (item,done)
        {
        },
        function (err)
        {
           cb(_.filter(items,function (i) { return !i._txTransient; }));
        });
     },opts)
    .consumed(p.trigger.consumed)
    .error(p.trigger.error);

     return p;
  };
};
