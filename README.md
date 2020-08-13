# WorkspaceTesi


# Clustering Folder

## KMEANS
* *kmeans.py*: this is the script to get the clustering results through the KMEANS algorithm. It accepts such arguments as input:
    
    1. the csv path of the features;
    2. the minimum value of 'n_cluster' ('k', number of cluster);
    3. the max value of 'n_cluster' ('k', number of cluster);
    4. the scaler to use ('1' for the StandardScaler,'2' for the MinMax Scaler, other value we apply the clustering without scaling data);
    5. 'loop' is a integer flag to set to 1 to iterate the Kmenas algorithm from the minimun to the max value of 'n_cluster';
    6. 'top_scale' is a integer flag to set to '1' if we want consider the feature 'A/AAA_top_country' in the scaling phase, '0' if we don't want consider them in the scaling phase;
    7. 'save_fig' is a integer flag to set to '1' if we want save the pair plots, '0' if we don't want save them.
## DBSCAN
* *dbscan.py*: this is the script to get the clustering results through the DBSCAN algorithm. It accepts such arguments as input:

    1. the csv path of the feature;
    2. 'metric' is a string value to say the distance matric to use ('euclidean','l1','l2', etc.);
    3. 'max_eps' is the max value of 'epsilon'. The script iterate from 0.1 to 'max_eps - 0.1' with a step of 0.1;
    4. the minimum number of 'min_samples' (The number of samples (or total weight) in a neighborhood for a point to be considered as a core point. This includes the point itself);
    5. the maximum number of 'min_samples' (The number of samples (or total weight) in a neighborhood for a point to be considered as a core point. This includes the point itself);
    6. the scaler to use ('1' for the StandardScaler,'2' for the MinMax Scaler, other value we apply the clustering without scaling data);
    7. 'top_scale' is a integer flag to set to '1' if we want consider the feature 'A/AAA_top_country' in the scaling phase, '0' if we don't want consider them in the scaling phase;
    8. 'loop' is a integer flag to set to 1 to iterate the DBSCAN algorithm from the minimun to the max value of 'eps' and ' min_samples'.

##  HDBSCAN
* *c_hdbscan.py*: this is the script to get the clustering results through the HDBSCAN algorithm.  It accepts such arguments as input:

    1. the csv path of the feature;
    2. the minimum value of 'min_cluster_size';
    3. the max value of 'min_cluster_size';
    4. the scaler to use ('1' for the StandardScaler,'2' for the MinMax Scaler, other value we apply the clustering without scaling data);
    5. 'loop' is a integer flag to set to 1 to iterate the HDBSCAN algorithm from the minimun to the max value of 'min_cluster_size';
    6. 'top_scale' is a integer flag to set to '1' if we want consider the feature 'A/AAA_top_country' in the scaling phase, '0' if we don't want consider them in the scaling phase;
    7. 'save_fig' is a integer flag to set to '1' if we want save the pair plots, '0' if we don't want save them.

## PCA
* *pca.py*: this is the script to apply the PCA with a predefined number of principal components. It accepts:

    1. the csv path fo the feature;
    2. the number of pc;
    3. the scaler to use ('1' for the StandardScaler,'2' for the MinMax Scaler, other value we apply the clustering without scaling data);
    4. the name of csv file that we will obtain in output;
    5. 'saving' set to 1 we wil obtain the csv with the components specified;
    6. 'top_scale' is a integer flag to set to '1' if we want consider the feature 'A/AAA_top_country' in the scaling phase, '0' if we don't want consider them in the scaling phase;

# Data Set Preparation Folder