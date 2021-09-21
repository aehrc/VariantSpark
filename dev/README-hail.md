Info on Hail Integration (hail 0.2)
===================================


## Building and deploying hail jar 

### Building

Follow the instructions from: https://hail.is/docs/0.2/getting_started_developing.html

Checkout a specific tag

Build with:

	./gradlew -Dscala.version=2.12.14 -Dspark.version=3.1.1 -Delasticsearch.major-version=7 clean jar shadowJar
	
(`spark.version` needs to be the one variant-spark is currently compiled with)

The outputs are in `build/libs` : `hail.jar`, `hail-all-spark.jar`


### Deploying to maven 

The deployment is currently to: `https://oss.sonatype.org/content/repositories/snapshots` 
because of lower requiremnts (no need for pgp signatures etc).

Relevant passwords need to be set in: `~/.m2/settting.xml`

To deploy use:

	./misc-deploy-hail-to-maveh.sh <hail-root> 
	
where `<hail-root>` is the location of hail clone with  `jar` and `shadowJar` built.

## Expression resolution



	mt = data.annotate_cols(label = labels[data.s])
	mt.count()

Resolver into evaluation of:

	(TableCount
	  (MatrixColsTable
	    (MatrixMapCols None
	      (MatrixAnnotateColsTable "__uid_44"
	        (MatrixRead None False False "{\"name\":\"MatrixVCFReader\",\"files\":[\"../data/hipsterIndex/hipster.vcf.bgz\"],\"callFields\":[\"PGT\"],\"entryFloatTypeName\":\"Float64\",\"rg\":\"GRCh37\",\"contigRecoding\":{},\"arrayElementsRequired\":true,\"skipInvalidLoci\":false,\"gzAsBGZ\":false,\"forceGZ\":false,\"filterAndReplace\":{\"name\":\"TextInputFilterAndReplace\"},\"partitionsJSON\":null}")
	        (TableKeyBy (samples) False
	          (TableRead None False "{\"name\":\"TextTableReader\",\"options\":{\"files\":[\"../data/hipsterIndex/hipster_labels.txt\"],\"typeMapStr\":{\"label\":\"Int64\",\"score\":\"Float64\"},\"comment\":[],\"separator\":\",\",\"missing\":[\"NA\"],\"noHeader\":false,\"impute\":false,\"quoteStr\":null,\"skipBlankLines\":false,\"forceBGZ\":false,\"filterAndReplace\":{\"name\":\"TextInputFilterAndReplace\"},\"forceGZ\":false}}")))
	      (InsertFields
	        (SelectFields (s)
	          (Ref sa))
	        None
	        (label
	          (GetField __uid_44
	            (Ref sa)))))))
	            
	            
	            
	            
	2019-07-12 12:18:34 root: INFO: optimize: before: IR size 82: 
	(TableMapRows
	  (MatrixToTableApply "{\"name\":\"LinearRegressionRowsSingle\",\"yFields\":[\"__y_0\"],\"xField\":\"__uid_102\",\"covFields\":[\"__cov0\"],\"rowBlockSize\":16,\"passThrough\":[]}"
	    (MatrixRename () () ("__uid_103" "__uid_104") ("__y_0" "__cov0") () () ("__uid_105") ("__uid_102")
	      (MatrixMapEntries
	        (MatrixMapCols None
	          (MatrixMapRows
	            (MatrixMapCols None
	              (MatrixMapCols ()
	                (MatrixMapEntries
	                  (MatrixMapCols None
	                    (MatrixMapCols None
	                      (MatrixAnnotateColsTable "__uid_44"
	                        (MatrixRead None False False "{\"name\":\"MatrixVCFReader\",\"files\":[\"../data/hipsterIndex/hipster.vcf.bgz\"],\"callFields\":[\"PGT\"],\"entryFloatTypeName\":\"Float64\",\"rg\":\"GRCh37\",\"contigRecoding\":{},\"arrayElementsRequired\":true,\"skipInvalidLoci\":false,\"gzAsBGZ\":false,\"forceGZ\":false,\"filterAndReplace\":{\"name\":\"TextInputFilterAndReplace\"},\"partitionsJSON\":null}")
	                        (TableKeyBy (samples) False
	                          (TableRead None False "{\"name\":\"TextTableReader\",\"options\":{\"files\":[\"../data/hipsterIndex/hipster_labels.txt\"],\"typeMapStr\":{\"label\":\"Int64\",\"score\":\"Float64\"},\"comment\":[],\"separator\":\",\",\"missing\":[\"NA\"],\"noHeader\":false,\"impute\":false,\"quoteStr\":null,\"skipBlankLines\":false,\"forceBGZ\":false,\"filterAndReplace\":{\"name\":\"TextInputFilterAndReplace\"},\"forceGZ\":false}}")))
	                      (InsertFields
	                        (SelectFields (s)
	                          (Ref sa))
	                        None
	                        (label
	                          (GetField __uid_44
	                            (Ref sa)))))
	                    (InsertFields
	                      (SelectFields (s label)
	                        (Ref sa))
	                      None
	                      (__uid_103
	                        (GetField score
	                          (GetField label
	                            (Ref sa))))
	                      (__uid_104
	                        (F64 1.0))))
	                  (InsertFields
	                    (SelectFields (GT)
	                      (Ref g))
	                    None
	                    (__uid_105
	                      (ApplyIR toFloat64
	                        (Apply nNonRefAlleles
	                          (GetField GT
	                            (Ref g)))))))
	                (SelectFields (s label __uid_103 __uid_104)
	                  (Ref sa)))
	              (SelectFields (label __uid_103 __uid_104)
	                (SelectFields (s label __uid_103 __uid_104)
	                  (Ref sa))))
	            (SelectFields (locus alleles)
	              (MakeStruct
	                (locus
	                  (GetField locus
	                    (Ref va)))
	                (alleles
	                  (GetField alleles
	                    (Ref va)))
	                (rsid
	                  (GetField rsid
	                    (Ref va)))
	                (qual
	                  (GetField qual
	                    (Ref va)))
	                (filters
	                  (GetField filters
	                    (Ref va)))
	                (info
	                  (GetField info
	                    (Ref va))))))
	          (SelectFields (__uid_103 __uid_104)
	            (SelectFields (label __uid_103 __uid_104)
	              (Ref sa))))
	        (SelectFields (__uid_105)
	          (SelectFields (GT __uid_105)
	            (Ref g))))))
	  (InsertFields
	    (SelectFields (locus alleles n sum_x y_transpose_x beta standard_error t_stat p_value)
	      (Ref row))
	    None
	    (y_transpose_x
	      (ApplyIR indexArray
	        (GetField y_transpose_x
	          (Ref row))
	        (I32 0)))
	    (beta
	      (ApplyIR indexArray
	        (GetField beta
	          (Ref row))
	        (I32 0)))
	    (standard_error
	      (ApplyIR indexArray
	        (GetField standard_error
	          (Ref row))
	        (I32 0)))
	    (t_stat
	      (ApplyIR indexArray
	        (GetField t_stat
	          (Ref row))
	        (I32 0)))
	    (p_value
	      (ApplyIR indexArray
	        (GetField p_value
	          (Ref row))
	        (I32 0)))))
	2019-07-12 12:18:34 root: INFO: optimize: after: IR size 61:
