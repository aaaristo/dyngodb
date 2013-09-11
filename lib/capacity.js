var _= require('underscore');

module.exports= function (rcurrent, rtarget, wcurrent, wtarget)
{
    var _steps= function (current, target)
        {
            var diff= target-current, 
                steps= diff>0 ? (diff > current*2 ? [] : [target]) : [target];

            if (!steps.length)
            {
               while ((current=current*2)<target)
                   steps.push(current);

               var delta= target-steps[steps.length-1];

               if (delta>0)
                 steps.push(target);
            }

            return steps; 
        },
        _fill= function (arr,len)
        {
           var diff= len-arr.length,
               last= arr[arr.length-1];

           _(diff).times(function () { arr.push(last); });
        };

    var rsteps= _steps(rcurrent,rtarget),
        wsteps= _steps(wcurrent,wtarget);

    if (rsteps.length>wsteps.length)
      _fill(wsteps,rsteps.length);
    else
    if (wsteps.length>rsteps.length)
      _fill(rsteps,wsteps.length);

    return _.zip(rsteps,wsteps);
}
