# variantspark_script.py
import varspark as vs
from pyspark.sql import SparkSession

# Step 1: Create a Spark session with VariantSpark JAR attached
spark = SparkSession.builder.config('spark.jars', vs.find_jar()).getOrCreate()

# Step 2: Create a VarsparkContext
vc = vs.VarsparkContext(spark, silent=True)

# Step 3: Load features and labels
features = vc.import_vcf('/app/VariantSpark/data/chr22_1000.vcf')
labels = vc.load_label('/app/VariantSpark/data/chr22-labels.csv', '22_16050408')

# Optional: Print some information to verify
print("Features loaded:", features)
print("Labels loaded:", labels)

# Step 4: Run the importance analysis and retrieve top important variables:
ia = features.importance_analysis(labels, seed = 13, n_trees=500, batch_size=20)
top_variables = ia.important_variables()

# Step 5: Display the results.
print("%s\t%s" % ('Variable', 'Importance'))
for var_and_imp in top_variables:
    print("%s\t%s" % var_and_imp)    

# Stop the Spark session
spark.stop() 

