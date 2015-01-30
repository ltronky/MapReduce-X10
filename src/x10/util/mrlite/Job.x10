package x10.util.mrlite;
import x10.util.Pair;
import x10.util.ArrayList;

public interface Job[K1,V1,K2,V2,K3,V3] {

	def partition(k:K2):Long;
	
	def stop():Boolean;

	def source():Iterable[Pair[K1,V1]];

	def sink(Iterable[Pair[K3,V3]]):void;
	
	def mapper(K1, V1, (K2,V2)=>void):void;
	
	def reducer(K2,Iterable[V2], ArrayList[Pair[K3,V3]]):void;
}