var _= require('underscore'),
    ret= require('ret'),
    http= require('http'),
    url= require('url'),
    querystring= require('querystring'),
    colors= require('colors'),
    async= require('async'),
    AWS = require('aws-sdk');

const  _oa = function(o, s) 
       {
             s = s.replace(/\[(\w+)\]/g, '.$1'); // convert indexes to properties
             s = s.replace(/^\./, '');           // strip a leading dot
             var a = s.split('.');
             while (a.length) {
                 var n = a.shift();
                 if (n in o) {
                     o = o[n];
                 } else {
                     return;
                 }
             }
             return o;
       };

module.exports= function (dyn,table,fields)
{
     var index= {}, domain= {}, fieldNames= _.keys(fields);

     if (fieldNames[0]!='$search') return false;

     var CS= new AWS.CloudSearch();

     domain= fields[fieldNames[0]];

     if (!domain.domain||!domain.lang) return;

     index.name= 'CloudSearch-'+(domain.name= domain.domain);
     delete domain.domain;

     domain.post= function (elem,done)
     {
       var post_data= JSON.stringify([elem]),
           post_options = {
                              hostname: domain.aws.DocService.Endpoint,
                              port: 80,
                              path: '/2011-02-01/documents/batch',
                              method: 'POST',
                              headers: {
                                          'Accept': 'application/json',
                                          'Content-Type': 'application/json; charset=UTF-8',
                                          'Content-Length': post_data.length
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
                done(null,JSON.parse(json)); 
              }); 
       })
       .on('error', done);

       post_req.write(post_data);
       post_req.end();

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
           elem.type='add';
           domain.post(elem,done);
         }
         else
           done(); 
     };

     index.update= function (item)
     {
          index.put(item,function (err, res) 
          { 
             if (err)
               console.log((err+'').red,err.stack); 
             else
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
         if (index.indexable(item))
         {
           var elem= index.makeElement(item);
           elem.type='del';
           domain.post(_.omit(elem,'fields'),done);
         }
         else
           done();
     };
     
     index.indexable= function (item)
     {
        return _.some(_.keys(item),function (field) { return field.indexOf('$')!=0 });
     };

     index.usable= function (cond)
     {
        return !!cond.$search;
     };

     index.makeElement= function (item)
     {
        var elem= { id: item.$id+'__'+item.$pos, version: item.$version, fields: {} };
        elem.lang= item.$lang || domain.lang;

        _.keys(item).forEach(function (field)
        {
            if (field.indexOf('$')!=0)
              elem.fields[field]= item[field];
        });

        return elem;
     };

     index.find= function (query)
     {
       var p= dyn.promise('results');

       domain.get(query.cond.$search, function (err, res)
       {
          if (err) p.trigger.error(err);
          else
            p.trigger.results(_.collect(res.hits.hit,
            function (hit)
            { 
                var id= hit.id.split('__');
                return { $id: id[0], $pos: parseInt(id[1]) }; 
            })); 
       });        

       return p;
     };

     return index; 
};

