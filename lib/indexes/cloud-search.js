var _= require('underscore'),
    ret= require('ret'),
    http= require('http'),
    url= require('url'),
    querystring= require('querystring'),
    colors= require('colors'),
    async= require('async'),
    Stream= require('stream').Stream,
    AWS = require('aws-sdk');

const IDRE= /^[a-z0-9][a-z0-9_]*$/;

module.exports= function (dyn,table,fields)
{
     var index= {}, domain= {}, fieldNames= _.keys(fields);

     if (fieldNames[0]!='$search') return false;

     var CS= new AWS.CloudSearch();

     domain= fields[fieldNames[0]];

     if (!domain.domain||!domain.lang) return;

     index.name= 'CloudSearch-'+(domain.name= domain.domain);
     delete domain.domain;

     var postQueue= [];

     setInterval(function poster()
     {
       var _post= function (elems)
           {
               var post_data= JSON.stringify(_.pluck(elems,'elem')),
                   post_options = {
                                      hostname: domain.aws.DocService.Endpoint,
                                      port: 80,
                                      path: '/2011-02-01/documents/batch',
                                      method: 'POST',
                                      headers: {
                                                  'Accept': 'application/json',
                                                  'Content-Type': 'application/json; charset=UTF-8',
                                                  'Content-Length': Buffer.byteLength(post_data)
                                               }
                                  };

               var post_req = http.request(post_options, function(res) 
               {
                  res.setEncoding('utf8');
                  var json= '';
                  res.on('data', function (chunk) 
                      {
                          json+=chunk;
                      })
                     .on('end', function ()
                      {
                        var res= JSON.parse(json); 
                        elems.forEach(function (e) { e.done(null,res); });
                      }); 
               })
               .on('error', function (err)
               {
                     elems.forEach(function (e) { e.done(err); });
               });

               post_req.write(post_data);
               post_req.end();
           };

         var cnt=0, elems= [];

         while (cnt++<1000&&postQueue.length)
            elems.push(postQueue.shift());

         if (elems.length)
           _post(elems);

     },1000);

     domain.post= function (elem,done)
     {
        postQueue.push({ elem: elem, done: done });
     };

     domain.stream= function ()
     {
             var wstream= new Stream(),
                 emit= { 
                         drain: _.bind(wstream.emit,wstream,'drain'),
                         error:  _.bind(wstream.emit,wstream,'error'),
                         finish:  _.bind(wstream.emit,wstream,'finish')
                       },
                 _ops= function (items)
                 {
                    return _.filter(_.collect(items,function (item)
                    { 
                         var elem= index.makeElement(item);
                          
                         if (elem)
                         {
                           elem.type='add';
                           return elem; 
                         }
                         else
                           return undefined; 
                           
                    }),function (item) { return !!item; });
                 },
                 write= function (items,emit)
                 {
                     async.forEach(_ops(items),domain.post,
                     function (err)
                     {
                        if (err)
                          emit.error(err);
                        else
                          emit.done();
                     });
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

     domain.get= function (query,done)
     {
            var _query= 'http://'+domain.aws.SearchService.Endpoint+'/2011-02-01/search?'
                         +querystring.stringify(query);

            //console.log(_query);

            http.get(_query,
            function(res) 
            {
               res.setEncoding('utf8');
               
               var json= '';
               res.on('data', function (chunk) 
                  {
                      json+=chunk;
                  })
                  .on('end', function ()
                  {
                      // 2xx status codes indicate that the request was processed successfully.
                      if ((res.statusCode+'').indexOf('2')==0)
                        done(null,JSON.parse(json)); 
                      else
                      if (res.statusCode==404)
                        done(new Error('not found'));
                      else
                      if (res.statusCode==405)
                        done(new Error('Invalid HTTP Method'));
                      else
                      if (res.statusCode==408)
                        done(new Error('Request Timeout'));
                      else
                      if (res.statusCode==500)
                        done(new Error('Internal Server Error'));
                      else
                      if (res.statusCode==502)
                        done(new Error('Search service is overloaded'));
                      else
                      if (res.statusCode==504)
                        done(new Error('Search service is overloaded, retry later'));
                      else
                      if (res.statusCode==507)
                        done(new Error('Insufficient Storage'));
                      else
                      if (res.statusCode==509)
                        done(new Error('Bandwidth Limit Exceeded'));
                      else
                      if ((res.statusCode+'').indexOf('4')==0)
                        done(new Error('Malformed request: '+res.statusCode));
                      else
                      if ((res.statusCode+'').indexOf('5')==0)
                        done(new Error('CloudSearch is experiencing problems: '+res.statusCode));
                      else
                        done(new Error('Unknown status code from CloudSearch: '+res.statusCode));
                  }); 
            })
            .on('error', done);
     };

     index.exists= function (done)
     {
         CS.describeDomains({ DomainNames: [domain.name] },function(err, data)
         {
            if (err) done(err);
            else
               done(null,!!_.filter(data.DomainStatusList,
                                    function (d)
                                    { 
                                        if (d.DomainName==domain.name&&d.Created&&!d.Deleted)
                                        { 
                                            domain.aws= d; 
                                            return true;
                                        } 
                                    }).length);
         });
     };

     index.drop= function (done)
     {
         CS.deleteDomain({ DomainName: domain.name },
         function (err,data)
         {
            if (err) done(err);
            else
              (function check()
               {
                  index.exists(function (err,exists)
                  {
                      if (err)
                        done(err);
                      else
                      if (exists)
                        setTimeout(check,5000);
                      else
                        done();
                  });
               })();  
         });
     };

     index.create= function (done)
     {
         console.log('this operation may take several minutes'.yellow);

         CS.createDomain({ DomainName: domain.name },
         function (err,data)
         {
            if (err) done(err);
            else
              (function check()
               {
                  index.exists(function (err,exists)
                  {
                      if (err)
                        done(err);
                      else
                      if (!exists||domain.aws.Processing)
                      {
                        if (domain.aws.Processing)
                          console.log(('CloudSearch is initializing domain: '+domain.name).yellow);

                        setTimeout(check,5000);
                      }
                      else
                       table.find().results(function (items)
                       {
                           async.forEach(items,index.put,done);
                       })
                       .error(done);
                  });
               })();  
         });
     };

     index.ensure= function (done)
     {
         index.exists(function (err, exists)
         { 
            if (err)
              done(err);
            else
            if (exists)
            {
               /*if (domain.aws.Processing)
               {
                   console.log(('CloudSearch is configuring domain: '+domain.name).yellow);
                   setTimeout(function () { index.ensure(done); },5000);
               }
               else*/
                 done();
            }
            else
              index.create(done);
         });
     };

     index.put= function (item,done)
     {
         if (index.indexable(item))
         {
           var elem= index.makeElement(item);

           if (elem)
           {
               elem.type='add';
               domain.post(elem,done);
           }
           else
             done(); 
         }
         else
           done(); 
     };

     index.update= function (item,op) // @FIXME: should give errors to the indexer?
     {
          _.bind(op=='put' ? index.put : index.remove,index)(item,function (err, res) 
          { 
             if (err)
               console.log((err+'').red,err.stack); 
             else
             if (res)
             {
                if (res.status=='error')
                  res.errors.forEach(function (err) { console.log(err.message.red); });
                else
                if (res.warnings)
                  res.warnings.forEach(function (warn) { console.log(warn.message.yellow); });
             }
          });

          return undefined;
     };

     index.remove= function (item,done)
     {
         var p= done ? { trigger: { success: done } } : dyn.promise(null,null,'consumed');

         if (index.indexable(item))
         {
           var elem= index.makeElement(item);

           if (elem)
           {
               elem.type='del';
               domain.post(_.omit(elem,'fields'),p.trigger.success);
           }
           else
               process.nextTick(p.trigger.success);
         }
         else
           process.nextTick(p.trigger.success);

         return p;
     };
     
     index.indexable= function (item)
     {
        return _.some(_.keys(item),function (field) { return field.indexOf('_')!=0 });
     };

     index.usable= function (query)
     {
        return !!query.cond.$search;
     };

     index.makeElement= function (_item)
     {
        var item= domain.transform ? domain.transform(_item) : _item,
            elem= { id: item._id.replace(/\./g,'_dot_').replace(/@/g,'_at_').replace(/-/g,'_')+'__'+item._pos, version: item._rev, fields: {} };

        elem.lang= item._lang || domain.lang;

        _.keys(item).forEach(function (field)
        { 
            var val= item[field];

            if (field.indexOf('_')!=0&&typeof val!='object'&&field!='__jsogObjectId')
              elem.fields[field.toLowerCase()]= val;
        });

        if (_.keys(elem.fields).length&&IDRE.exec(elem.id))
          return elem;
        else
          return undefined;
     };

     index.rebuild= function (window)
     {
           var p= dyn.promise(),
               sindex= dyn.stream(index.name),
               pcnt= 0;

           dyn.stream(table._dynamo.TableName)
              .scan({ limit: window })
              .on('data',function (items) { process.stdout.write(('\r'+(pcnt+=items.length)).yellow); })
              .pipe(domain.stream())
              .on('error',p.trigger.error)
              .on('finish',_.compose(p.trigger.success,console.log));

           return p;
     };

     index.find= function (query)
     {
       var p= dyn.promise(['results','count','end'],null,'consumed');

       query.limited= query.canLimit();

       if (query.limited)
       {
           if (query.limit) 
             query.cond.$search.size= query.limit;
            
           if (query.skip)
             query.cond.$search.start= query.skip;
       }

       domain.get(query.cond.$search, function (err, res)
       {
          if (err) p.trigger.error(err);
          else
          {
            delete query.$filter['$search'];

            query.counted= query.canCount();

            if (query.count&&query.counted)
                p.trigger.count(res.hits.hit.length);
            else
            {
                p.trigger.results(_.collect(res.hits.hit,
                function (hit)
                { 
                    var id= hit.id.split('__');
                    return { _id: id[0].replace(/_dot_/,'.').replace(/_at_/,'@').replace(/_/g,'-'), _pos: parseInt(id[1]) }; 
                })); 
                p.trigger.end();
            }
          }
       });        

       return p;
     };

     return index; 
};

