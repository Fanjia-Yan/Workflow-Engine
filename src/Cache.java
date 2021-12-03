import java.util.Comparator;
import java.util.Date;
import java.util.PriorityQueue;

public class Cache {
    public final int capacity;
    public int remaining_capacity;
    public PriorityQueue<blob> cache;

    /** initialize a Cache with capacity bytes */
    public Cache(int capacity){
        this.capacity = capacity;
        this.remaining_capacity = capacity;
        cache = new PriorityQueue<>();
    }

    /** Cache eviction policy(Least Recent Used):
     * If the cache is full and not able to stored the next workflow/operator,
     * we evicts the cache(s) that is/are least recently use.
     * The implementation includes a Priority Queue that polls that blob
     * using Date as comparator
     *
     * @Design_trade_off:
     *  My design ideology is that because some workflows are executed on a daily or weekly basis
     *  so it will be important keep the one that is executed recently
     *  The potential alternative is Least Frequent Used Eviction policy, those that used once or twice
     *  should be evicted more recently
     *
     */
    private void cache_eviction(int memory_size){
        while(memory_size <= remaining_capacity){
            blob b = cache.poll();
            remaining_capacity += b.sizeof();
        }
    }

    /** add the workflow/ operator to the cache
     *
     * @param id : the id of the operation/ workflow
     * @param serialized_data : the serializable data that stored
     */
    public void add(String id, byte[] serialized_data){
        int memory_size = id.getBytes().length + serialized_data.length;
        if(remaining_capacity - memory_size <= 0){
            cache_eviction(memory_size);
        }
        blob b = new blob(id,serialized_data);
        cache.add(b);
        remaining_capacity = remaining_capacity - memory_size;
    }

    /** find the workflow/ operator in the cache, return null if it's a cache miss
     *
     * @param id : the id of the operation/ workflow
     */
    public byte[] find(String id){
        for(blob b : cache){
            if (b.id.equals(id)){
                return b.serializable_data;
            }
        }
        return null;
    }

    /** update the workflow or operator based on current time
     *
     * @param id : the id of the operation/ workflow
     */
    public void update(String id){
        for(blob b:cache){
            if(b.id.equals(id)){
                b.date = new Date(System.currentTimeMillis());
            }
        }
    }

    /** flush the cache
     *
     */
    public void flush(){
        this.remaining_capacity = capacity;
        this.cache = new PriorityQueue<>();
    }

    /** blob class to put into the priority queue*/
    private class blob implements Comparable<blob>{
        public Date date;
        public String id;
        public byte[] serializable_data;

        public blob(String id, byte[]serializable_data){
            this.id = id;
            this.serializable_data = serializable_data;
            this.date = new Date(System.currentTimeMillis());
        }

        public int sizeof(){
            /** size of date + id + serializeable data */
            return 32 + id.getBytes().length + serializable_data.length;
        }

        @Override
        public int compareTo(blob o) {
            return this.date.compareTo(o.date);
        }

    }
}
