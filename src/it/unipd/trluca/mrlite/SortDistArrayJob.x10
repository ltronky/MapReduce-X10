package it.unipd.trluca.mrlite;

import x10.util.mrlite.Job;
import x10.util.Pair;
import x10.util.ArrayList;
import x10.util.mrlite.Engine;
import x10.array.DistArray_Block_1;
import x10.util.Random;
import x10.array.DistArray;
import x10.util.RailUtils;
import x10.util.HashMap;
import x10.util.Team;


public class SortDistArrayJob(origArr:DistArray_Block_1[Long], destArray:DistArray_Block_1[Pair[Long,Long]]) 
   implements Job[Long,Long,Long,Pair[Long, Long],Long,ArrayList[Pair[Long,Long]]] {
	var i:Long=0;
	public def stop():Boolean=i++ > 0;
	var max:Long;
	var min:Long;
	
	
	public def source()= new Iterable[Pair[Long,Long]]() {
		public def iterator() = new Iterator[Pair[Long,Long]]() {
			val data = origArr.localIndices();
			var i:Long=0;
			public def hasNext() = data.min(0)+i <= data.max(0);
			public def next() = Pair[Long,Long](data.min(0)+i, origArr(data.min(0) + i++));
		};
	};
	
	public def partition(k:Long)=k;
	
	//Utility variables for collecting the sub-results
	val keyRail:GlobalRef[Rail[Long]] = GlobalRef[Rail[Long]](new Rail[Long](Place.numPlaces()));
	val hashM:GlobalRef[HashMap[Long, ArrayList[Pair[Long,Long]]]] =
		GlobalRef[HashMap[Long, ArrayList[Pair[Long,Long]]]](new HashMap[Long, ArrayList[Pair[Long,Long]]](Place.numPlaces()));
	
	public def sink(s:Iterable[Pair[Long, ArrayList[Pair[Long,Long]]]]): void {
		//Collect at Place(0) all the ordered sub-arrays
		val aPos = here.id;
		for (x in s) {
			val temp = x;
			keyRail.evalAtHome((r:Rail[Long])=>r(aPos) = temp.first);
			hashM.evalAtHome((r:HashMap[Long, ArrayList[Pair[Long,Long]]])=>r.put(temp.first, temp.second));
		}
		
		//Once collected all
		Team.WORLD.barrier();

		//Only in Place(0) create the result DistArray
		if (here == Place(0)) {
			Console.OUT.println("MidSink t=" + System.nanoTime());
			RailUtils.sort(keyRail(), (i:Long,j:Long)=>(i-j) as Int);
			var sortArr:ArrayList[Pair[Long,Long]] = new ArrayList[Pair[Long,Long]]();
			for (i in 0..(keyRail().size-1)) {
				sortArr.addAll(hashM().get(keyRail()(i)));
			}
			
			val portion = sortArr.size() / Place.numPlaces();
			var rest:Long = sortArr.size() % Place.numPlaces();
			var start:Long = 0;
			
			finish for (i in 0..(Place.numPlaces()-1)) {
				val pp = portion + ((rest>0) ? 1 : 0);
				val piece = sortArr.subList(start, start+pp);
				at (Place(i)) async {
					val li = destArray.localIndices();
					for (j in 0..(piece.size()-1))
						destArray(li.min(0)+i) = piece(j);
				}
				start += pp;
				rest--;
			}
		}
	}
	
	public def mapper(k:Long, v:Long, s:(Long,Pair[Long, Long])=>void):void {
		if (Place.numPlaces()>1) {
			val span = max-min;
			s(v / (span/ (Place.numPlaces()-1)), Pair[Long, Long](v,k));
		} else {
			s(0, Pair[Long, Long](v,k));
		}
	}

	public def reducer(a:Long, b:Iterable[Pair[Long, Long]], sink:ArrayList[Pair[Long, ArrayList[Pair[Long,Long]]]]):
		void {
		if (b !=null) {
			var r:ArrayList[Pair[Long,Long]] = new ArrayList[Pair[Long,Long]]();
			for (x in b) r.add(x);
			r.sort((i:Pair[Long,Long],j:Pair[Long,Long])=>(i.first-j.first) as Int);
			sink.add(Pair(r(0).first, r));
		}
	}
	
	public static def test0(args:Rail[String]) {
		val N = args.size > 0 ? Long.parseLong(args(0)) : 1000;
		Console.OUT.println("N=" + N);
		val random = new Random();
		val originArray = new DistArray_Block_1[Long](N, (Long)=>(new Random()).nextLong(1000000L));
		
		//Print the original array
		// try {
		// 	Console.OUT.print("{");
		// 	finish for (p in Place.places()) at(p) async {
		// 		//Console.OUT.println("Starting at " + here);
		// 		for (i in originArray.localIndices()) {
		// 			val s ="(" + originArray(i) + " at=" + i + " Place=" + originArray.place(i).id + "),";
		// 			at (Place(0)) {
		// 				Console.OUT.print(s);
		// 			}
		// 		}
		// 		Console.OUT.flush();
		// 		//Console.OUT.println("Done with " + here);
		// 	}
		// 	Console.OUT.println("}");
		// } catch (z:Exception) {
		// 	Console.OUT.println("Aha! " + z);
		// 	z.printStackTrace();
		// }
		//Array Initializazion Completed
		val job=new SortDistArrayJob(originArray, new DistArray_Block_1[Pair[Long,Long]](N));
		
		//Find min and max values
		job.max = finish(Reducible.MaxReducer[Long](Long.MIN_VALUE)) {
			for(p in Place.places()) at(p) async {
				var localMax:Long = Long.MIN_VALUE;
				for (i in originArray.localIndices()) {
					if (originArray(i) > localMax) localMax = originArray(i);
				}
				offer localMax;
			}
		};
		job.min = finish(Reducible.MinReducer[Long](Long.MAX_VALUE)) {
			for(p in Place.places()) at(p) async {
				var localMin:Long = Long.MAX_VALUE;
				for (i in originArray.localIndices()) {
					if (originArray(i) < localMin) localMin = originArray(i);
				}
				offer localMin;
			}
		};
		Console.OUT.println("GLOBAL MIN -> " + job.min + ", GLOBAL MAX -> " + job.max);
		
		//Execute the Job
		new Engine(job).run();
		
		//Print out the result
		// Console.OUT.print("S{");
		// finish for (p in Place.places()) at(p) async {
		// 	for (i in job.destArray.localIndices()) {
		// 		val s = "S(" + job.destArray(i) + " at=" + i + " Place=" + originArray.place(i).id + "),";
		// 		at (Place(0)) {
		// 			Console.OUT.print(s);
		// 		}
		// 	}
		// 	Console.OUT.flush();
		// }
		// Console.OUT.println("}S");
	}

	public static def main(args:Rail[String]) {
		test0(args);
	}
}