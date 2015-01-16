/*

A HyperLog-sandwich

High cardinality approximation for cumulative frequency partitioning in stream compute.

  HL(Bf) | HL(Bf) | ...... | HL(Bf)

  -> (HyperLogLog(i))(BloomFilter(i)) ->

*/
var utils = require('./lib/utils.js');
var HyperLogSandwich = require('./lib/hyperlog-sandwich');

var max = 15;
var trials = 5;
var i, bf, hll, hll2, fCounts, output = [];

// Generate a buncha data
for(var t = 1; t < trials ; t++) {
  for(var s = 0; s < 5; s++) {
  var epsilon = 1/(Math.pow(10,t));
  var hls = new HyperLogSandwich(max, null, null, null, null, epsilon);
  console.log(epsilon);
  var inputSet    = []
    , streamSize  =  100000;

  (function (i) {
    // console.log('Generating dataset');
    for (i = 0; i <= streamSize; i++) {
      inputSet.push(Math.random().toString());
    }
    inputSet = utils.insertDups(inputSet, max);
    stream = inputSet;
  })();

  // HyperLogSandwich
  var frequency, item;
  (function () {
    // console.log('Inserting HyperLogSandwich');
    for(i = 0; i < stream.length; i++) {
      if (i % 100000 == 0) {
        console.log(t + '/' + trials, (Math.floor(i / stream.length * 1000) / 10) + '%');
      }
      hls.add(stream[i]);
    }
  })();

  var freqMap = {};
  var freqMapCount = {};
  var fCounts = {};

  // calculate old fashioned way
  (function () {
    // console.log('Calculating exact counts')
    for(i = 0; i < stream.length; i ++) {
      if(!freqMap[stream[i]]) {
        freqMap[stream[i]] = 1;
      } else {
        freqMap[stream[i]]++;
      }
    }

    for(i = 0; i < max; i++) {
      for(j in freqMap) {
        if(freqMap[j] > i) {
          if(!freqMapCount[i]) {
            freqMapCount[i] = 1;
          } else {
            freqMapCount[i]++;
          }
        }
      }
    };
  })();

  function calcError(a, b) {
    return (Math.floor(10000 * (Math.abs(a - b) / b)) / 100) || 0;
  };

  // Output the results
  (function () {
    var i, avgErrHLS = 0, avgErrBloom = 0, avgErrCMS = 0, errCMS = [];
    var cmsDepth = [];
    cmsDepth.push(epsilon);
    // console.log(['Freq', 'Est', 'BChain', 'CMS', 'Actual', 'Err%', 'Err2%', 'Err3%'].join(',\t'));

    for(i = 0; i < max; i++) {
      var estimate  = hls.get(i) || 0;
      var actual    = freqMapCount[i] || 0;
      var cms       = hls.getCMSCount(i) || 0;
      var bChain    = hls.getBloomCount(i) || 0;

      var errorHLS    = calcError(estimate, actual);
      var errorBloom  = calcError(bChain, actual);
      var errorCMS    = calcError(cms, actual);

      avgErrCMS   += errorCMS;
      avgErrBloom += errorBloom;
      avgErrHLS   += errorHLS;

      errCMS.push(errorCMS);

      // console.log([i, estimate, bChain, cms, actual, errorHLS, errorBloom, errorCMS].join(',\t') + '%');
      cmsDepth.push(errorCMS);
    }
    // console.log(cmsDepth.join(', '));
    var medianErrorCMS = errCMS.sort()[Math.floor(errCMS.length / 2)];
    function s (a) { return Math.floor(a * 1000) / 1000; }
    // output.push([streamSize, s(avgErrCMS/max), medianErrorCMS]);
    output.push(cmsDepth);
    // console.log([streamSize, s(avgErrCMS/max), s(avgErrBloom/max), s(avgErrHLS/max)].join(', '));
  })();
}}

var out = {}
for(i = 0; i < output.length; i++) {
  // console.log(output[i].join(','));
  if (!out[output[i][0]]) {
    out[output[i][0]] = [];
  }
  out[output[i][0]].push(output[i]);
}
for(o in out) {
  arr = out[o][0];
  for(i = 1; i < out[o].length; i++) {
    for(k = 0 ; k < out[o][0].length; k++) {
      arr[k]+= out[o][i][k];
    }
  }
  out[o] = arr;
  for(i = 0; i < out[o].length; i++) {
    out[o][i] = out[o][i]/5;
  }
}

for(i in out) {
  console.log(out[i].join(', '));
}
// console.log(out);
