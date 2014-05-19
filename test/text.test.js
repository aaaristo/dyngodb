var should= require('chai').should(),
    assert= require('chai').assert,
    AWS = require('aws-sdk'),
    _= require('underscore'),
    fs= require('fs'),
    dyngo=  require('../index.js');

const _noerr= function (done)
      {
         return function (err)
         {
             if (err) console.log(err,err.stack);
             should.not.exist(err);
             done();
         };
      },
      accept= function (code,done)
      {
         return function (err)
         {
           if (err.code==code)
             done(); 
           else
             done(err);
         };
      },
      profiles= function ()
      {
        return JSON.parse(fs.readFileSync('test/sample.small.json','utf8'));
      };


describe('text',function ()
{
       var db;

       before(function (done)
       {
            dyngo({ dynamo: { endpoint: new AWS.Endpoint('http://localhost:8000') }, hints: false },
            function (err,_db)
            {
               db= _db; 

               db.test.remove().success(function ()
               {
                   var ensure= function ()
                   {
                       return db.test.ensureIndex
                       ({ 
                           name: 'S',
                           $text: function (item)
                           { 
                              return _.pick(item,['name','company','about']);
                           } 
                       })
                   };

                   ensure().success(function ()
                   {
                     db.test.indexes[0].drop().success(function ()
                     {
                         ensure().success(function ()
                         {
                            var p= profiles();

                            db.test.save(p)
                                   .success(done)
                                   .error(done);                               
                         }).error(done);
                     }).error(done);
                   }).error(_noerr(done));
               })
               .error(_noerr(done)); 

            });
       });

       it('Can do bloodhound like full text searches', function (done)
       {
             db.test.findOne({ $text: 'interdum adi' }) // inspired by bloodhound search with whitespace tokenizer
                    .result(function (obj)
                    {
                          obj.name.should.equal('Duncan Wall'); 
                          done();
                    })
                    .error(_noerr(done));
       });

       it('Can chain full text searches with normal fields', function (done)
       {
             db.test.findOne({ name: 'Duncan Wall', $text: 'interdum adi' })
                    .result(function (obj)
                    {
                         obj.name.should.equal('Duncan Wall'); 

                         db.test.findOne({ name: 'Hedley Booth', $text: 'interdum adi' })
                                .result(function (obj)
                                {
                                      should.not.exist(obj);
                                      done();
                                })
                                .error(accept('notfound',done));
                    })
                    .error(_noerr(done));
       });

       it('Should not find any result', function (done)
       {
             db.test.findOne({ $text: 'Xinterdum adi' }) // inspired by bloodhound search with whitespace tokenizer
                    .result(function (obj)
                    {
                          should.not.exist(obj);
                          done();
                    })
                    .error(accept('notfound',done));
       });
});
