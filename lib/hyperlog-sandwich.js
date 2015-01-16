var HyperLogLog = require('hyperloglog');
var CMS         = require("count-min-sketch");
var BloomFilter = require('bloomfilter').BloomFilter;

function HyperLogSandwich (depth, functions, bits, registers, epsilon, error) {
  var i, bf, hll, hll2;

  // Configs to tune
  var _functions = functions || 7;
  var _registers = registers || 12;
  var _bits      = bits      || 958506;
  var _cmsA      = epsilon   || 0.00001;
  var _cmsB      = error     || 0.001;

  this.max   = depth || 15;

  this.HLLs  = [];
  this.HLL2  = [];
  this.BFs   = [];
  this.cms   = CMS(_cmsA, _cmsB);
  this.fCounts = {};

  // Init all the counters
  for (i = 0; i < this.max; i++) {
    // init the BloomFilters
    bf = new BloomFilter(_bits, _functions);

    // init the HyperLogLogs
    hll = HyperLogLog(_registers);
    hll2 = HyperLogLog(_registers);

    this.HLLs.push(hll);
    this.HLL2.push(hll2);
    this.BFs.push(bf);
  }
}

// Insert new data into the HyperLogSandwich
HyperLogSandwich.prototype.add = function (item) {
  var frequency, hll, hll2, bf, cmsF, itemHash;

  // Strategy #1
  // Immediately update sketchmap
  var itemHash = HyperLogLog.hash(item);
  this.cms.update(itemHash, 1);
  cmsF = this.cms.query(itemHash) - 1;

  // Always insert into the 0th HLL, since the CSM will become saturated later
  this.HLL2[0].add(itemHash);
  if(cmsF < this.max) {
    this.HLL2[cmsF].add(itemHash);
  }

  // Strategy #2
  // Use a LinkedList of BloomFilters
  for(frequency = 0 ; frequency < this.max; frequency++) {
    hll = this.HLLs[frequency];
    bf  = this.BFs[frequency];

    // Count at least one occurance of this item
    hll.add(itemHash);

    // Have we (maybe) seen this item yet?
    if (!bf.test(item)) {
      // Record each time we set the BloomFilter (strategy #3)
      if(!this.fCounts[frequency]) {
        this.fCounts[frequency] = 0;
      }
      this.fCounts[frequency]++;

      // Set this depth's BloomFilter
      bf.add(item);
      break;
    }
  }
}

HyperLogSandwich.prototype.get = function(frequency) {
  return this.HLLs[frequency].count();
}

HyperLogSandwich.prototype.getCMSCount = function(frequency) {
  return this.HLL2[frequency].count();
}

HyperLogSandwich.prototype.getBloomCount = function(frequency) {
  return this.fCounts[frequency];
}

module.exports = HyperLogSandwich;