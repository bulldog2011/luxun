package com.leansoft.luxun.producer;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import com.leansoft.luxun.broker.Broker;
import com.leansoft.luxun.broker.BrokerInfo;
import com.leansoft.luxun.broker.ConfigBrokerInfo;
import com.leansoft.luxun.common.annotations.ClientSide;
import com.leansoft.luxun.common.exception.InvalidPartitionException;
import com.leansoft.luxun.common.exception.NoBrokersForTopicException;
import com.leansoft.luxun.producer.async.CallbackHandler;
import com.leansoft.luxun.producer.async.EventHandler;
import com.leansoft.luxun.serializer.Encoder;
import com.leansoft.luxun.utils.Utils;

import static com.leansoft.luxun.utils.Closer.closeQuietly;

/**
 * Message producer
 * 
 * @author bulldog
 *
 * @param <K> partitioner key
 * @param <V> real data
 */
@ClientSide
public class Producer<K, V> implements IProducer<K, V> {
	
	ProducerConfig config;
	
	private IPartitioner<K> partitioner;
	
	ProducerPool<V> producerPool;
	
	boolean populateProducerPool;
	
	BrokerInfo brokerInfo;
	
    private final AtomicBoolean hasShutdown = new AtomicBoolean(false);

    private final Random random = new Random();

    private Encoder<V> encoder;
    
    public Producer(ProducerConfig config, IPartitioner<K> partitioner, ProducerPool<V> producerPool, boolean populateProducerPool,
            BrokerInfo brokerInfo) {
        super();
        this.config = config;
        this.partitioner = partitioner;
        this.producerPool = producerPool;
        if(this.producerPool == null) {
        	this.producerPool =  new ProducerPool<V>(config, getEncoder());
        }
        this.populateProducerPool = populateProducerPool;
        this.brokerInfo = brokerInfo;
        
        if (this.brokerInfo == null) {
        	this.brokerInfo = new ConfigBrokerInfo(config.getBrokerList());
        }
        
        // pool of producers, one per broker
        if (this.populateProducerPool) {
        	List<Integer> brokerIdList = this.brokerInfo.getBrokerIdList();
        	for(Integer brokerId : brokerIdList) {
        		Broker b = this.brokerInfo.getBrokerInfo(brokerId);
                this.producerPool.addProducerForBroker(new Broker(brokerId, b.host, b.host, b.port));
        	}
        }
    }
    
    /**
     * This constructor can be used when all config parameters will be
     * specified through the ProducerConfig object
     * 
     * @param config Producer Configuration object
     */
    public Producer(ProducerConfig config) {
        this(config, //
        		null, //
                null, //
                true, //
                null);
    }    
    
    /**
     * This constructor can be used to provide pre-instantiated objects for
     * all config parameters that would otherwise be instantiated via
     * reflection. i.e. encoder, event handler and callback
     * handler. If you use this constructor, encoder, eventHandler,
     * and callback handler will not be picked up from the
     * config.
     * 
     * @param config Producer Configuration object
     * @param encoder Encoder used to convert an object of type V to
     *        binary data. If this is null it throws an
     *        InvalidConfigException
     * @param eventHandler the class that implements
     *        luxun.producer.async.EventHandler<T> used to dispatch a
     *        batch of produce requests, using an instance of
     *        luxun.producer.SyncProducer. If this is null, it uses the
     *        DefaultEventHandler
     * @param cbkHandler the class that implements
     *        luxun.producer.async.CallbackHandler<T> used to inject
     *        callbacks at various stages of the
     *        luxun.producer.AsyncProducer pipeline. If this is null, the
     *        producer does not use the callback handler and hence does not
     *        invoke any callbacks
     * @param partitioner class that implements the
     *        luxun.producer.Partitioner<K>, used to supply a custom
     *        partitioning strategy on the message key (of type K) that is
     *        specified through the ProducerData<K, T> object in the send
     *        API. If this is null, producer uses DefaultPartitioner
     */
    public Producer(ProducerConfig config, Encoder<V> encoder, EventHandler<V> eventHandler, CallbackHandler<V> cbkHandler, IPartitioner<K> partitioner) {
        this(config, //
        		partitioner,
                new ProducerPool<V>(config, encoder, eventHandler, cbkHandler), //
                true, //
                null);
    }    
    
    @SuppressWarnings("unchecked")
    @Override
    public Encoder<V> getEncoder() {
        return encoder == null ?(Encoder<V>) Utils.getObject(config.getSerializerClass()):encoder;
    }
    
    public void send(ProducerData<K, V> data) throws NoBrokersForTopicException {
        if (data == null) return;
        configSend(data);
    }

    private void configSend(ProducerData<K, V> data) {
        producerPool.send(create(data));
    }
    
    private ProducerPoolData<V> create(ProducerData<K, V> pd) {
        List<Integer> brokerIdList = brokerInfo.getBrokerIdList();
        if (brokerIdList == null || brokerIdList.size() == 0) {
            throw new NoBrokersForTopicException("Topic= " + pd.getTopic());
        }

        int numBrokers = brokerIdList.size();
        int index = pd.getKey() == null ? 
        		random.nextInt(numBrokers) : this.getPartitioner().partition(pd.getKey(), numBrokers);
        		
        if (index < 0 || index >= numBrokers) {
            throw new InvalidPartitionException("Invalid broker index : " + index + "\n Valid values are in the range inclusive [0, " + (numBrokers - 1)
                    + "]");
        }		
        		
        int brokerId = brokerIdList.get(index);
        		
        return this.producerPool.buildProducerPoolData(pd.getTopic(),//
               brokerInfo.getBrokerInfo(brokerId), pd.getData());
    }
    
	@Override
	public void close() {
        if (hasShutdown.compareAndSet(false, true)) {
            closeQuietly(producerPool);
            closeQuietly(brokerInfo);
        }
	}

	@SuppressWarnings("unchecked")
	@Override
	public IPartitioner<K> getPartitioner() {
        if(partitioner == null) {
            partitioner =  (IPartitioner<K>) Utils.getObject(config.getPartitionerClass());
        }
        return partitioner;
	}
}
