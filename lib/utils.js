/**
 * Since we don't really care about the input set for this
 * analysis, just generate a uniformly distributed 32bit string
 *
 * Returns a value n E [0.. 2^(n-1)]
 */
function generateRandom32bitStream () {
  var val = []
    , n
    , nthBit
    , binaryString;

  for (n = 0; n < 32; n++) {
    nthBit = ((Math.random() > 0.5) + 0);
    val.push(nthBit);
  }
  binaryString = val.join('');
  return binaryString;
}

/**
 * A naive O(n) space, time cardinality check.
 * calculates the exact cardinality;
 */
function cardinality (set) {
  var counted = {}
    , count = 0
    , i
    , index;

  for (i = 0; i < set.length; i++) {
    index = set[i];
    if (!counted[index]) {
      count++;
    }
    counted[index] = true;
  }
  return count;
}

/**
 * Generate a n-set of uniformly distributed
 * 32-bit binary strings which simulates the
 * output of running n-data points through a
 * hash function f with uniform hash distribution
 * into binary space.
 */
function getUniformBitStream (num) {
  var output = [];
  for (num; num > 0; --num) {
    output.push(generateRandom32bitStream());
  }
  return output;
};

/**
 * For the sake of experimentation, forcibly
 * insert some duplicates to our dataset.
 */
function insertDups (set) {
  var dups = []
    , i;

  for (i = set.length - 1; i >= 0; --i) {
    if (Math.random() < 0.25) {
      dups.push(set[i]);
    }
  }

  // we really don't care about the order
  return set.concat(dups);
}

module.exports = {
  generateRandom32bitStream:  generateRandom32bitStream,
  cardinality:                cardinality,
  getUniformBitStream:        getUniformBitStream,
  insertDups:                 insertDups
};