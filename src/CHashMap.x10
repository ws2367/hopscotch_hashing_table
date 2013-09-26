/******************************************************************************
*
* Concurrent Hopscotch HashMap
*
* Course Project, COMS 4130, Parallel Programming, Fall 2012
*    
* Columbia University
*
* Ref: Hopscotch Hashing  Maurice Herlihy, Nir Shavit and Moran Tzafrir
*
* by Daniel Perlmutter <dperlmut@gmail.com>, Joaquin Ruales <joaqo182@gmail.com>, and Wen-Hsiang Shaw <wenhshiang.Shaw@gmail.com>
*
*******************************************************************************/

import x10.io.Console;
import x10.util.concurrent.Lock;
import x10.util.concurrent.AtomicLong;
import x10.util.Timer;
import x10.util.Random;
import x10.util.Box;
import x10.util.ArrayList;

public class CHashMap[K, V] {

  /*========= static variables =========
  MAX_SEGMENTS:         The maximum number of segments. See the comments in grow()
  RESIZE_FACTOR:        The ratio of size of the original hash table and the desired size of the resized hash
                        table, whether it is growing or shrinking
  MAX_OPTIMISTIC_TRIES: The maximum number of tries that lookup conducts the optimistic search(search through the hop info).
                        After the maximum number of optimistic tries, it conducts pessimistic search.
  LOAD_FACTOR:          The initial ratio of number of elements that will be added over the hash table size
  ======================================*/
  private static val MAX_SEGMENTS         = 1000000;
  private static val RESIZE_FACTOR        = 2;
  private static val MAX_OPTIMISTIC_TRIES = 2;
  private static val LOAD_FACTOR          = 0.1;
  private static val isDebugging          = false;
  
  //========= instance variables =========
  private var buckets:Rail[CEntry[K, V]];
  private var numSegments:Int = 1;
  private var rand:Random     = new Random(System.nanoTime());
  private var offset:Int      = 0;
  private var lastResizeTime:AtomicLong;
  private var DEFAULT_NUM_BUCKETS:Int;
  private var NEIGHBORHOOD_SIZE:Int;
  
  //========= class methods =========
  public def this(numNeighbors:Int, numElements:Int) {
    NEIGHBORHOOD_SIZE   = numNeighbors;
    DEFAULT_NUM_BUCKETS = (numElements as Double/LOAD_FACTOR) as Int;
    buckets = new Rail[CEntry[K,V]](DEFAULT_NUM_BUCKETS, (i:Int)=>new CEntry[K,V]());
    //lastResizeTime = new AtomicLong(Timer.nanoTime());
  }  

  //Given a key computes which bucket to put it in
  private def getBucketIndexFromKey(key:K):Int {
    val hash<:Int = posMod(key.hashCode(), buckets.size);
    val segment<:Int = posMod(hash, numSegments);
    val bucket<:Int = hash / numSegments;
    return DEFAULT_NUM_BUCKETS * segment + bucket;
  }

  private def posMod(m:Int, n:Int) {
    var modVal:Int = m % n;
    if (modVal < 0) {
      modVal += n;
    }
    return modVal;
  }

  //A bucket is considered empty if it has no entry or the entry has a null key
  //A bucket is also considered empty if the virtual bucket associated with its key does not indicate this bucket to be filled in its bitmap
  //The value of a bucket is irrelevant if it is empty
  private def empty(bucket:Int):Boolean {
    return empty(bucket, buckets);
  }

  //See above
  private def empty(bucket:Int, currentBuckets:Rail[CEntry[K, V]]):Boolean {
    return (currentBuckets(bucket) == null || currentBuckets(bucket).getKey() == null || currentBuckets(bucket).getKey()() == null);
  }

  //Attempts to get the actual bucket associated with the given key.  Fails if an add or remove alters the bitmap of the virtual bucket associated with the given key
  public def tryGetActualBucket(key:K):Int {
    val virtualBucket<:Int = getBucketIndexFromKey(key);
    val bitmap = buckets(virtualBucket).getBitmap();
    for (var index:Int = 0; index < bitmap.size(); index++) {
      val offset = bitmap.get(index);
      val testBucket = virtualBucket + offset;
      if(testBucket >=0 && testBucket < buckets.size && !empty(testBucket) && buckets(testBucket).getKey()().equals(key)) {
        return testBucket;
      }
    }
    return -1;
  }

  //Returns the actual bucket associated with a given key, or -1 if the key is not in the hash map 
  public def getActualBucket(key:K):Int {
    val virtualBucket<:Int = getBucketIndexFromKey(key);
    for (var offset:Int = 0; offset < NEIGHBORHOOD_SIZE; offset++) {
      val testBucket = virtualBucket + offset;
      if(testBucket >=0 && testBucket < buckets.size && !empty(testBucket) && buckets(testBucket).getKey()().equals(key)) {
        return testBucket;
      }
    }
    return -1;
  }
  
  //A bucket is considered initialized if it is a non-null reference
  private def initialized(bucket:Int):Boolean {
    return initialized(bucket, buckets);
  }

  //See above
  private def initialized(bucket:Int, currentBuckets:Rail[CEntry[K, V]]):Boolean {
    return (currentBuckets(bucket) != null);
  }

  //Moves the value in the new bucket to the empty bucket and updates the virtual bucket's bitmap
  private def swap(newBucket:Int, emptyBucket:Int) {
    if(isDebugging) Console.OUT.print("SWAPPING " + newBucket + " AND " + emptyBucket + "...");
    val key<:K = buckets(newBucket).getKey()();
    val value = buckets(newBucket).getValue()();
    buckets(emptyBucket).setKey(key);
    buckets(emptyBucket).setValue(value);
    buckets(emptyBucket).setTimestamp();
    //buckets(emptyBucket) = new CEntry[K,V](key, value);
    val virtualBucket<:Int = getBucketIndexFromKey(key);
    buckets(virtualBucket).addBit(emptyBucket - virtualBucket);
    buckets(virtualBucket).removeBit(newBucket - virtualBucket);
    buckets(virtualBucket).setTimestamp();
    buckets(newBucket).setKeyNull();
    if(isDebugging) Console.OUT.print("SWAP DONE ");
  }

  //Hops the empty bucket back to be closer to the desired bucket
  //Returns the index of the new empty bucket of -1 if the empty bucket could not be hopped back
  private def hop(desiredBucket:Int, emptyBucket:Int):Int {
    if(isDebugging) Console.OUT.print("HOPPING " + emptyBucket + " BACK TO " + desiredBucket + "...");
    var newBucket:Int = emptyBucket;
    for (var index:Int = Math.max(emptyBucket - NEIGHBORHOOD_SIZE + 1, desiredBucket); index < emptyBucket; index++) {
      if (empty(index)) {
        newBucket = index;
        break;
      } else {
        val delta<:Int = buckets(index).getFirstEntry();
        if (delta >= 0  && (index + delta) < emptyBucket) {
          newBucket = index + delta;
          break;
        }
      }
    } 
    if (newBucket < emptyBucket) { 
      if (!empty(newBucket)) {
        swap(newBucket, emptyBucket);
      }
    }
    if(isDebugging) Console.OUT.println("HOP DONE");
    return newBucket;
  }

  //Adds a given key and value to the hash table
  public def add(key:K, value:V) {
    var currentBucket:Int;
    var tempTime:Long = Timer.nanoTime();
    var oldBuckets:Rail[CEntry[K,V]] = buckets;
    var bucket:Int = getBucketIndexFromKey(key);
    //if a resize occurred while we were acquiring the locks, try again
    //while(true) {
      if(isDebugging) Console.OUT.println("ACQUIRING LOCKS FOR BUCKET " + bucket + "'S NEIGHBORHOOD");
      getLocksOfAllNeighbors(bucket);
      
      /*if(lastResizeTime.get() < tempTime) {
      if(lastResizeTime.get()<tempTime) {
        break;
      } else {// there was a resize when we were acquiring our locks
        if(isDebugging) Console.OUT.println("CONFLICT!");
        releaseLocksOfAllNeighbors(bucket, oldBuckets);
        tempTime = Timer.nanoTime();
        oldBuckets = buckets;
        bucket = getBucketIndexFromKey(key);
      }*/
    //}
    
    //at this point we have the locks and all is well
    if(isDebugging) Console.OUT.println("TRYING TO ADD TO BUCKET " + bucket + " VALUE " + value);
      currentBucket = bucket;
      
    //Check if key is already in the HashMap, then replace value if it is
    val actualBucket<:Int = tryGetActualBucket(key);
    if(actualBucket != -1) {
      //the key already exists so replace its value
      buckets(actualBucket).setValue(value);
      buckets(actualBucket).setTimestamp();
      if(isDebugging) Console.OUT.println("ADDED TO " + currentBucket + " with value " + buckets(currentBucket).getValue()());
      releaseLocksOfAllNeighbors(bucket);
      //releaseAllLocks();
      return;
    }
      
    //Find an empty bucket
    while (currentBucket < buckets.size && !empty(currentBucket)) {
      currentBucket++;
    }
   
    //Hop the empty bucket back and place the new entry in it
    if (currentBucket >= buckets.size) { //out-of-bounds case. grow takes care of atomicity and such
      /* releaseLocksOfAllNeighbors(bucket); //it is really important to release these locks before the resize, since resize needs to acquire all the locks
      //releaseAllLocks();
      grow();
      add(key, value);
      return;
      if(isDebugging) Console.OUT.println("ADDED TO " + currentBucket + " with value " + (buckets(currentBucket).getValue())());
      //buckets(bucket).releaseLock();
      releaseLocksOfAllNeighbors(bucket); //linearization point
      //releaseAllLocks();*/
      throw new RuntimeException("Refusing to grow.");
    } else {
      // we're gonna hop so we need more locks
      // we currently hold the locks in the current bucket's neighborhood.
      val rangeMax<:Int = currentBucket;

      getLocksInRange(bucket+NEIGHBORHOOD_SIZE, rangeMax);

      //note that we need not check for resize again here, since the locks we have are prevent it from happening. It is imperative that all lock range acquisitions scan from left to right.
      // we might need to check for additions and deletions, though.
      while (true) {
        val newBucket = hop(bucket, currentBucket);
        if (newBucket == currentBucket) {
          if (newBucket < bucket + NEIGHBORHOOD_SIZE) {
            buckets(newBucket).setKey(key);
            buckets(newBucket).setValue(value);
            buckets(newBucket).setTimestamp();
            buckets(bucket).addBit(newBucket - bucket);
            buckets(bucket).setTimestamp();
            if(isDebugging) Console.OUT.println("ADDED TO " + bucket + " with value "+ (buckets(bucket).getValue())());
            releaseLocksInRange(bucket, Math.max(rangeMax, bucket + NEIGHBORHOOD_SIZE - 1));
            return;
          } else {
            throw new RuntimeException("Couldn't hop.");
            /*releaseLocksInRange(bucket, rangeMax);
            //releaseLocksOfAllNeighbors(bucket);
            //releaseAllLocks();
            grow();
            add(key, value);
            return;
            //currentBucket = bucket;
            */
          }
        }
        currentBucket = newBucket;
      }
    }
  }
    
  //Returns the value associated with the given key, or null if nonexistent
  public def get(key:K) {
    val virtualBucket = getBucketIndexFromKey(key);
    var actualBucket:Int = 0;
    var tryCounter:Int = 0;
    var timestamp:Long = 0;
    //Optimistically try to do lookup assuming nothing else interferes ("fast path")
    do {
      timestamp = buckets(virtualBucket).getTimestamp();
      actualBucket = tryGetActualBucket(key);
      tryCounter++;
    } while (timestamp != buckets(virtualBucket).getTimestamp() && tryCounter < MAX_OPTIMISTIC_TRIES);
    if (timestamp != buckets(virtualBucket).getTimestamp()) {
      //Optimistic attempts failed so we must try look up pessimistically ("slow path") 
      actualBucket = getActualBucket(key);
    }
    return (actualBucket == -1) ? null : (buckets(actualBucket).getValue())();
  }

  //Removes the entry with the given key from the hash table.
  //Returns the value associated with the removed key or null if no entry has the given key
  public def remove(key:K) {    
    var tempTime:Long = Timer.nanoTime();
    var oldBuckets:Rail[CEntry[K,V]] = buckets;
    var virtualBucket:Int = getBucketIndexFromKey(key);
    if(oldBuckets(virtualBucket) == null) {
      return null;
    }
    
    //if a resize occurred while we were acquiring the lock, try again
    //while(true) {
      if(isDebugging) Console.OUT.println("ACQUIRING LOCK FOR BUCKET " + virtualBucket);
      oldBuckets(virtualBucket).getLock();
      
      /*if(lastResizeTime.get()<tempTime) {
        break;
      } else {// there was a resize when we were acquiring our locks
        if(isDebugging) Console.OUT.println("CONFLICT! TRYING AGAIN");
        oldBuckets(virtualBucket).releaseLock();
        tempTime = Timer.nanoTime();
        oldBuckets = buckets;
        virtualBucket = getBucketIndexFromKey(key);
        if(oldBuckets(virtualBucket)==null) return null;
      }*/
    //}
     
    if(isDebugging) 
      Console.OUT.println("TRYING TO REMOVE KEY " + key);

    val actualBucket<:Int = getActualBucket(key);
    
    if(actualBucket == -1){
      if(isDebugging) Console.OUT.println("KEY NOT FOUND");
      buckets(virtualBucket).releaseLock();
      return null;
    } else {
      buckets(virtualBucket).removeBit(actualBucket - virtualBucket);
      val value = buckets(actualBucket).getValue()();
      buckets(actualBucket).setKeyNull();
      buckets(virtualBucket).setTimestamp();
      if(isDebugging) Console.OUT.println("RELEASING LOCK FOR BUCKET " + virtualBucket);
      buckets(virtualBucket).releaseLock();
      if(isDebugging) Console.OUT.println("KEY REMOVED");
      return value;
    } 
  }
  
  // Acquire every single lock. Useful when doing a hashmap-wide operation such as grow
  private def getAllLocks() {
    for (var i:Int = 0; i < buckets.size; i++) {
        if (!initialized(i)){
          // add new null entry
          buckets(i) = new CEntry[K,V]();
        }
          buckets(i).getLock();
    }
  }

  //Release every single lock
  private def releaseAllLocks() {
    releaseAllLocks(buckets);
  }
  //Release all the initialized allocks in the hash table
  private def releaseAllLocks(_buckets:Rail[CEntry[K, V]]) {
    for (var i:Int = 0; i < _buckets.size; i++) {
        if (initialized(i, _buckets)) 
          _buckets(i).releaseLock();
    }
  }
  
  //Acquires locks on all buckets in the given range  
  private def getLocksInRange(rangeMin:Int, rangeMax:Int) {
    for (var testBucket:Int = rangeMin; testBucket <= rangeMax; testBucket++) {
      if(testBucket >=0 && testBucket < buckets.size) {
        //if (!initialized(testBucket)){
          // add new null entry
        //  buckets(testBucket) = new CEntry[K,V]();
        //}
        buckets(testBucket).getLock();
      }
    }
  }

  //Release all locks in a given range
  private def releaseLocksInRange(rangeMin:Int, rangeMax:Int) {
    releaseLocksInRange(rangeMin, rangeMax, buckets);
  }
  
  private def releaseLocksInRange(rangeMin:Int, rangeMax:Int, _buckets:Rail[CEntry[K, V]]) {
    for (var testBucket:Int = rangeMin; testBucket <= rangeMax; testBucket++) {
      if(testBucket >=0 && testBucket < _buckets.size) {
        if (initialized(testBucket, _buckets)) { 
          _buckets(testBucket).releaseLock();
        }
      }
    }
  }
  
  //Acquires locks on all buckets in the neighborhood of the virtual bucket  
  private def getLocksOfAllNeighbors(virtualBucket:Int) {
    for (var offset:Int = 0; offset < NEIGHBORHOOD_SIZE; offset++) {
      val testBucket = virtualBucket + offset;
      if(testBucket >=0 && testBucket < buckets.size ) {
        //if (!initialized(testBucket)){
          // add new null entry
          //buckets(testBucket) = new CEntry[K,V]();
        //}
        buckets(testBucket).getLock();
      }
    }
  }
  
  //Releases locks on all buckets in the neighborhood of the virtual bucket
  private def releaseLocksOfAllNeighbors(virtualBucket:Int) {
    releaseLocksOfAllNeighbors(virtualBucket, buckets);
  }
  private def releaseLocksOfAllNeighbors(virtualBucket:Int, _buckets:Rail[CEntry[K, V]]) {
    for (var offset:Int = 0; offset < NEIGHBORHOOD_SIZE; offset++) {
      val testBucket = virtualBucket + offset;
      if(testBucket >=0 && testBucket < _buckets.size ) {
        if (initialized(testBucket, _buckets)) {
          _buckets(testBucket).releaseLock();
        }
      }
    }
  }

  //Rehash all items from the old backing array
  private def rehash(oldBuckets:Rail[CEntry[K,V]]) {
    for(var i:Int = 0; i < oldBuckets.size; i++) {
      if(!empty(i, oldBuckets)) {
        add(oldBuckets(i).getKey()(), oldBuckets(i).getValue()());
      }
    }
    lastResizeTime.set(Timer.nanoTime());
    for(var i:Int = 0; i < oldBuckets.size; i++) {
      if(!empty(i, oldBuckets)) {
      //When we release the old locks (which we'll do now),
      //if an insertion or a deletion was waiting on the lock,
      //it will now run.
        oldBuckets(i).releaseLock();
      }
    }
  }

  //Shrink the backing array by the resize factor
  private def shrink() { 
    if(isDebugging) Console.OUT.println("SHRINKING HASH MAP");

    //GET ALL THE LOCKS. Note that we'll rehash everything and build new entries, but we will still have to release all locks in old array after we're done rehashing, and make sure we detect if the array has grown in the remove and add methods
    val tempTime:Long = Timer.nanoTime();
    val oldBuckets<:Rail[CEntry[K,V]] = buckets;
    getAllLocks();
    if(lastResizeTime.get()>tempTime) { //oh man, someone did our work already!
    	releaseAllLocks(oldBuckets);
    	return;
    }
    
    buckets = new Rail[CEntry[K,V]](buckets.size / RESIZE_FACTOR, (i:Int)=>new CEntry[K, V]());
    numSegments /= RESIZE_FACTOR;
    lastResizeTime.set(Timer.nanoTime());
    rehash(oldBuckets);
  }
	
  //Grow the backing array by the resive factor
  private def grow() {
    if(isDebugging) Console.OUT.println("GROWING HASH MAP");
    
    //GET ALL THE LOCKS. Note that we'll rehash everything and build new entries, but we will still have to release all locks in old array after we're done rehashing, and make sure we detect if the array has grown in the remove and add methods
    val tempTime:Long = Timer.nanoTime();
    val oldBuckets<:Rail[CEntry[K,V]] = buckets;
    getAllLocks();
    if(lastResizeTime.get()>tempTime) { //someone did our work for us. 
    	releaseAllLocks(oldBuckets);
    	return;
    }
    
    buckets = new Rail[CEntry[K,V]](buckets.size * RESIZE_FACTOR, (i:Int)=>new CEntry[K, V]());
    numSegments *= RESIZE_FACTOR;
    if (numSegments > CHashMap.MAX_SEGMENTS) {
      throw new RuntimeException("Too many items hash to the same value.  Increase the neighborhood size or maximum number of segments.");
    }
    lastResizeTime.set(Timer.nanoTime());
    rehash(oldBuckets);
  }
}

class CEntry[K, V] {
  //========= instance variables =========
  private var key:Box[K];
  private var value:Box[V];
  private var bitmap:ArrayList[Int];
  private var lock:Lock;

  private var timestamp:Long;

  //========= class methods =========
  public def this() {
    key = null;
    value = null;
    //Ideally the bitmap should be a sparse min heap but x10 doesn't have built in heaps so we use an array list
    bitmap = new ArrayList[Int]();
    lock = new Lock();
    timestamp = Timer.nanoTime();
  }
  public def this(_key:K, _value:V) {
    this(_key, _value, new Lock());
  }
  
  public def this(_key:K, _value:V, _lock:Lock) {
    key = new Box[K](_key);
    value = new Box[V](_value);
    bitmap = new ArrayList[Int]();
    lock = _lock;
    timestamp = Timer.nanoTime();
  }

  public def getKey():Box[K]{
    return key;
  }
  public def setKey(_key:K) {
    key = new Box[K](_key);
  }	
  public def setKeyNull() {
    key = null;
  }	  
  public def getValue():Box[V] {
    return value;
  }
  public def setValue(_value:V) {
    value = new Box[V](_value);
  }

  public def getBitmap():ArrayList[Int] {
    return bitmap;
  }

  public def addBit(offset:Int) {
    bitmap.add(offset);
  }

  public def removeBit(offset:Int) {
    bitmap.remove(offset);
  }

  public def getLock() {
    lock.lock();
  }
  
  public def tryLock():Boolean {
    return lock.tryLock();
  }
  
  public def releaseLock() {
    lock.unlock();
  }
  
  public def setTimestamp() {
    timestamp = Timer.nanoTime();
  }
  
  public def getTimestamp():Long {
    return timestamp;
  }
  
  public def getLockVariable():Lock {
    return lock;
  }
  
  //Returns the offset of the closest actual bucket mapped to this virtual bucket or -1 if nothing is mapped to this virtual bucket
  public def getFirstEntry() {
    if (bitmap.isEmpty()) {
      return -1;
    } else {
      var first:Int = bitmap.get(0);
      for (var index:Int = 1; index < bitmap.size(); index++) {
        val current = bitmap.get(index);
        if (current < first) {
          first = current;
        }
      }
      return first;
    }
  }
}
