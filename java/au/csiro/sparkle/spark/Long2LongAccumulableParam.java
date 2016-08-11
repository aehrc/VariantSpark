package au.csiro.sparkle.spark;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import org.apache.spark.AccumulableParam;

import scala.Tuple2;

public class Long2LongAccumulableParam implements AccumulableParam<Long2LongOpenHashMap, Tuple2<Long,Long>> {


	private static final long serialVersionUID = 1L;

	@Override
	public Long2LongOpenHashMap addAccumulator(Long2LongOpenHashMap arg0,
			Tuple2<Long, Long> arg1) {
		arg0.addTo(arg1._1, arg1._2);
		return arg0;
	}

	@Override
	public Long2LongOpenHashMap addInPlace(final Long2LongOpenHashMap  arg0,
			Long2LongOpenHashMap arg1) {
		arg1.forEach((k,v) -> arg0.addTo(k, v));
		return arg0;
	}

	@Override
	public Long2LongOpenHashMap zero(Long2LongOpenHashMap arg0) {
		// TODO Auto-generated method stub
		return new Long2LongOpenHashMap();
	}
	

}
