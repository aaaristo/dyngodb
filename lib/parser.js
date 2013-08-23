var _= require('underscore'),
    ret= require('ret');

const _soa = function (o, s, v)
      {
            s = s.replace(/\[(\w+)\]/g, '.$1'); // convert indexes to properties
            s = s.replace(/^\./, '');           // strip a leading dot
            var a = s.split('.'),
                prop= a.pop();

            while (a.length) 
            {
                var n = a.shift();
                o = o[n] || (o[n]={});
            }

            o[prop]= v;
      },
      _oa = function(o, s) 
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
      },
      _buildFilter= function (query)
      {
           var cond= query.cond,
               filter= query.$filter= {},
               canFind= true;

           Object.keys(cond).every(function (field)
           {
                   var val= cond[field], type= typeof val,
                       _field= function (field,val,op)
                       {
                          filter[field]= { values: val===undefined ? [] : (Array.isArray(val) ? val : [ val ]),
                                               op: op };
                       },
                       _regexp= function (field,val)
                       {
                          var tks= ret(val.source),
                              _chars= function (start)
                              {
                                  return _.collect(tks.stack.slice(start),
                                           function (tk) { return String.fromCharCode(tk.value); })
                                          .join(''); 
                              };

                          if (tks.stack
                              &&tks.stack[0]
                              &&tks.stack[0].type==ret.types.POSITION
                              &&tks.stack[0].value=='^'
                              &&!_.filter(tks.stack.slice(1),function (tk) { return tk.type!=ret.types.CHAR; }).length)
                          {
                            var val= _chars(1);
                            if (val!='')
                              _field(field,val,'BEGINS_WITH');
                          }
                          else
                          if (tks.stack
                              &&!_.filter(tks.stack,function (tk) { return tk.type!=ret.types.CHAR; }).length)
                          {
                            var val= _chars(0);
                            if (val!='')
                              _field(field,val,'CONTAINS');
                          }
                          else
                              _field(field,val,'REGEXP');
                       };

                   if (type=='object')
                   {
                       if (Array.isArray(val))
                            _field(field,val,'IN');
                       else
                       {
                            if (val instanceof RegExp)
                              _regexp(field,val);
                            else
                            if (val.$ne!==undefined)
                              _field(field,val.$ne,'NE');
                            else
                            if (val.$gt!==undefined)
                              _field(field,val.$gt,'GT');
                            else
                            if (val.$lt!==undefined)
                              _field(field,val.$lt,'LT');
                            else
                            if (val.$gte!==undefined)
                              _field(field,val.$gte,'GE');
                            else
                            if (val.$lte!==undefined)
                              _field(field,val.$lte,'LE');
                            else
                            if (val.$in!==undefined)
                              _field(field,val.$in,'IN');
                            else
                            if (val.$exists!==undefined)
                              _field(field,undefined, val.$exists ? 'NOT_NULL' : 'NULL');
                            else
                              return canFind= false;
                       }
                   }
                   else
                     _field(field,val,'EQ');

                   return true;
           });

           return canFind;
      };


module.exports= function (dyn)
{
    var parser= {};

    parser.parse= function (table,modifiers,cond,projection,identity)
    {
        var p= dyn.promise('parsed'),
            query= { table: table, cond: cond || {}, projection: { root: {} } },
            _projection= function (root)
            {
              var proj= { include: ['$id','$ref'], exclude: []};

              _.keys(root).forEach(function (attr)
              {
                  if (root[attr].$include)
                  {
                    proj.include.push(attr);
                    proj.include.push('$$'+attr);
                    proj.include.push('$$$'+attr);
                  }
                  else
                  if (root[attr].$exclude)
                  {
                    proj.exclude.push(attr);
                    proj.exclude.push('$$'+attr);
                    proj.exclude.push('$$$'+attr);
                  }
              });

              if (proj.include.length==2)
                proj.include= undefined;

              return proj;
            };

        table.name= table._dynamo.TableName;
        table.indexes= table.indexes || [];

        query.project= _projection;
        query.soa= _soa;
        query.oa= _oa;

        if (modifiers.orderby)
        {
               var fields= [];

               Object.keys(modifiers.orderby).forEach(function (name)
               {
                   fields.push({ name: name, dir: modifiers.orderby[name] });
               });

               query.$orderby= fields;
        }

        query.toprojection= function (root)
        {
              var proj= {};

              _.keys(root || {}).forEach(function (attr)
              {
                  if (root[attr].$include)
                    proj[attr]= 1;
                  else
                  if (root[attr].$exclude)
                    proj[attr]= -1;
              });

              return proj;
        };

        if (projection&&_.keys(projection).length)
        {
          query.identity= {};

          query.identity.get= function ($id, $pos, done)
          {
             done();
          }

          query.identity.set= function (item)
          {
          }
        }
        else
        {
          if (identity&&identity.map)
              query.identity= identity;
          else
          {
              query.identity= {};
              query.identity.map= {};

              query.identity.get= function ($id, $pos, cb)
              {
                 var item= query.identity.map[$id+':'+$pos];

                 if (!item)
                 {
                   query.identity.map[$id+':'+$pos]= true;
                   cb();
                 }
                 else
                 if (item===true)
                   setTimeout(query.identity.get,100,$id,$pos,cb);
                 else
                   cb(item);
              }

              query.identity.set= function ($id, $pos, item)
              {
                 query.identity.map[$id+':'+$pos]= item;
              }
          }
        }
        
        process.nextTick(function ()
        {

            _.extend(query,modifiers);

            if (projection)
            {
                _.keys(projection).every(function (attr)
                { 
                       if (projection[attr]==1)
                         _soa(query.projection.root,attr,{ $include: true }); 
                       else
                       if (projection[attr]==-1)
                         _soa(query.projection.root,attr,{ $exclude: true }); 
                       else
                       {
                         p.trigger.error(new Error('unknown projection value '+JSON.stringify(projection[attr])));
                         return false;
                       }

                       return true;
                });

                _.extend(query.projection,query.project(query.projection.root));
            }

            query.$supported= _buildFilter(query);
            query.$filtered= [];

            p.trigger.parsed(query); 

        });

        return p;
    };

    return parser;
};
