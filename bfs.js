/*

A HyperLog-sandwich

High cardinality approximation for cumulative frequency partitioning in stream compute.

  HL(Bf) | HL(Bf) | ...... | HL(Bf)

  -> (HyperLogLog(i))(BloomFilter(i)) ->

*/
var utils       = require('./lib/utils.js');
var HyperLogLog = require('hyperloglog');
var CMS         = require("count-min-sketch");
var BloomFilter = require('bloomfilter').BloomFilter;

// Configs to tune
var _functions = 12;
var _bits      = 32 * 2048 * 5;
var _registers = 16;

var max   = 20;
var HLLs  = [];
var HLL2  = [];
var BFs   = [];
var cms   = CMS(.001, .1);
var fCounts = {};
var i, bf, hll, hll2;

// Generate a buncha data
var inputSet = [], streamSize = 100000;
(function (i) {
  console.log('Generating dataset');
  for (i = 0; i <= streamSize; i++) {
    inputSet.push(Math.random().toString());
  }

  console.log('Inserting dupes');
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
  console.log('Creating HyperLogSandwich');
  // Init all the counters
  for (i = 0; i < max; i++) {
    // init the BloomFilters
    bf = new BloomFilter(_bits, _functions);

    // init the HyperLogLogs
    hll = HyperLogLog(_registers);
    hll2 = HyperLogLog(_registers);

    HLLs.push(hll);
    HLL2.push(hll);
    BFs.push(bf);
  }

  for(i = 0; i < stream.length; i++) {
    if (i % 100000 == 0) { console.log( (Math.floor(i / stream.length * 1000) / 10) + '%'); }
    item = stream[i];

    cms.update(item, 1);

    for(frequency = 0 ; frequency < max; frequency++) {
      hll   = HLLs[frequency];
      hll2  = HLL2[frequency];
      bf = BFs[frequency];

      // Count at least one occurance of this item
      hll.add(HyperLogLog.hash(item));

      // Have we (maybe) seen this item yet?
      if (!bf.test(item)) {
        if(!fCounts[frequency]) {
          fCounts[frequency] = 0;
        }
        fCounts[frequency]++;
        bf.add(item);
        break;
      }

      // Bloom filter thinks we've seen it, let's verify with CMS
      if (cms.query(item) <= frequency) {
        // console.log('FALSE', cms.query(item), frequency);
      }
    }
  }
})();

var freqMap = {};
var freqMapCount = {};

// calculate old fashioned way
(function () {
  console.log('Calculating exact counts')
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

// (function () {
//   // console.log(stream);
//   console.log(['item\t\t', 'actual', 'cms'].join('\t'));
//   for(i in freqMap) {
//     console.log([i, freqMap[i], cms.query(i)].join('\t'));
//   }
// })();

// return;

// Output the results
(function () {
  var i, estimate, actual, error, bChain, error2;
  console.log(['Freq', 'Est', 'BChain', 'Actual', 'Err%', 'Err2%', 'min%', 'max%'].join('\t'));
  for(i = 0; i < max; i++) {
    estimate = HLLs[i].count() || 0;
    actual = freqMapCount[i] || 0;
    bChain = fCounts[i] || 0;
    bMin = Math.min(estimate, bChain);
    bMax = Math.max(estimate, bChain);

    error   = calcError(estimate, actual);
    error2  = calcError(bChain, actual);
    error3  = calcError(bMin, actual);
    error4  = calcError(bMax, actual);

    console.log([i, estimate, bChain, actual, error, error2, error3, error4].join('\t') + '%');
  }
})();
