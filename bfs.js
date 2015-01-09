/*

A HyperLog-sandwich

High cardinality approximation for cumulative frequency partitioning in stream compute.

  HL(Bf) | HL(Bf) | ...... | HL(Bf)

  -> (HyperLogLog(i))(BloomFilter(i)) ->

*/
var utils = require('./lib/utils.js');
var HyperLogSandwich = require('./lib/hyperlog-sandwich');

var max = 15;
var i, bf, hll, hll2, fCounts;

// Generate a buncha data
for(var t = 0; t < 10 ; t++) {
  var hls = new HyperLogSandwich(max);
  var inputSet    = []
    , streamSize  = t * 10000;//Math.pow(2, 5 + t);

  (function (i) {
    // console.log('Generating dataset');
    for (i = 0; i <= streamSize; i++) {
      inputSet.push(Math.random().toString());
    }

    // console.log('Inserting dupes');
    for (i = 0 ; i < 10; i++) {
      inputSet = utils.insertDups(inputSet);
    }
    // skew
    for (i = 0 ; i < 1000; i++) {
      inputSet.push(inputSet[0]);
      inputSet.push(inputSet[1]);
      if (Math.random() > .5) { inputSet.push(inputSet[2]); }
    }
    stream = inputSet;
    // console.log('dataset size: ', stream.length);
  })();
  // stream = ['1','2','3','4','5','6','9','9','9','9'];

  // HyperLogSandwich
  var frequency, item;
  (function () {
    // console.log('Inserting HyperLogSandwich');
    for(i = 0; i < stream.length; i++) {
      // if (i % 100000 == 0) { console.log( (Math.floor(i / stream.length * 1000) / 10) + '%'); }
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
    var i, avgErrHLS = 0, avgErrBloom = 0, avgErrCMS = 0;
    var cmsDepth = [];
    cmsDepth.push(streamSize);
    // console.log(['Freq', 'Est', 'BChain', 'CMS', 'Actual', 'Err%', 'Err2%', 'Err3%'].join('\t'));

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

      // console.log([i, estimate, bChain, cms, actual, errorHLS, errorBloom, errorCMS].join(',') + '%');
      cmsDepth.push(errorHLS);
    }
    console.log(cmsDepth.join(', '));

    function s (a) { return Math.floor(a * 1000) / 1000; }
    // console.log([streamSize, s(avgErrCMS/max), s(avgErrBloom/max), s(avgErrHLS/max)].join(', '));
  })();
}