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

    In output, if we set 'save_fig' to 0 we will have these folders:

    * *csv_kmeans*: contain csv files for each k in the cluster process, in these file there is an order in according to the cluster id;
    * *csv_not_sorted_by_cluster*: contain csv files in which the domains are not sorted by cluster id value;
    * *img_sse_avgsil_db*: contain the image that represents the trend curves of SSEs, mean value of Silhouette Scores and Davies-Bloudin indices as the value of k varies.
    * *list_score*: contain a three different '.txt' files, each one contain the value of respectly SSE, mean value of Silhouette Scores and Davies-Bloudin indices.

    Otherwise, if we set 'save_fig' to 1 we will have these folders:

    * We obtain K folders named as 'Cluster_X' (where X is the k value); each folder contains the respective pair plot, the scatter with features two by two and the sil plot. 
    * *csv_kmeans*: contain csv files for each k in the cluster process, in these file there is an order in according to the cluster id;
    * *csv_not_sorted_by_cluster*: contain csv files in which the domains are not sorted by cluster id value;
    * *img_sse_avgsil_db*: contain the image that represents the trend curves of SSEs, mean value of Silhouette Scores and Davies-Bloudin indices as the value of k varies.
    * *list_score*: contain a three different '.txt' files, each one contain the value of respectly SSE, mean value of Silhouette Scores and Davies-Bloudin indices.


## DBSCAN
* *dbscan.py*: this is the script to get the clustering results through the DBSCAN algorithm. It accepts such arguments as input:

    1. the csv path of the feature;
    2. 'metric' is a string value to say the distance matric to use ('euclidean','l1','l2', etc.);
    3. 'max_eps' is the max value of 'epsilon'. The script iterate from 0.1 to 'max_eps - 0.1' with a step of 0.1;
    4. the minimum number of 'min_samples' (The number of samples (or total weight) in a neighborhood for a point to be considered as a core point. This includes the point itself);
    5. the maximum number of 'min_samples' (The number of samples (or total weight) in a neighborhood for a point to be considered as a core point. This includes the point itself);
    6. the scaler to use ('1' for the StandardScaler,'2' for the MinMax Scaler, other value we apply the clustering without scaling data);
    7. 'top_scale' is a integer flag to set to '1' if we want consider the feature 'A/AAA_top_country' in the scaling phase, '0' if we don't want consider them in the scaling phase;
    8. 'save_fig' is a integer flag to set to '1' if we want save the scatter with feature two by two, '0' if we don't want save them.
    9. 'loop' is a integer flag to set to 1 to iterate the DBSCAN algorithm from the minimun to the max value of 'eps' and ' min_samples'.

    In output, if we set 'save_fig' to 0 we will have these folders:

    * *csv_dbscan_not_sorted_by_cluster*: contain csv files in which the domains are not sorted by cluster id value;
    * *lists_score: contain a three different '.txt' files, each one contain the  mean value of Silhouette Scores and Davies-Bloudin indices.
    * *Stat_sil_db_mdbscan: contain the image that represents the trend curves of mean value of Silhouette Scores and Davies-Bloudin indices as the value of 'eps' varies, fixed 'min_cluster' value.

    In output, if we set 'save_fig' to 1 we will have these folders:

    * Folders named as 'Cluster_X_Y', where X is the 'min_cluster' value and Y is the 'eps' value.
    * *csv_dbscan_not_sorted_by_cluster*: contain csv files in which the domains are not sorted by cluster id value;
    * *lists_score: contain a three different '.txt' files, each one contain the  mean value of Silhouette Scores and Davies-Bloudin indices.
    * *Stat_sil_db_mdbscan: contain the image that represents the trend curves of mean value of Silhouette Scores and Davies-Bloudin indices as the value of 'eps' varies, fixed 'min_cluster' value.



##  HDBSCAN
* *c_hdbscan.py*: this is the script to get the clustering results through the HDBSCAN algorithm.  It accepts such arguments as input:

    1. the csv path of the feature;
    2. the minimum value of 'min_cluster_size';
    3. the max value of 'min_cluster_size';
    4. the scaler to use ('1' for the StandardScaler,'2' for the MinMax Scaler, other value we apply the clustering without scaling data);
    5. 'loop' is a integer flag to set to 1 to iterate the HDBSCAN algorithm from the minimun to the max value of 'min_cluster_size';
    6. 'top_scale' is a integer flag to set to '1' if we want consider the feature 'A/AAA_top_country' in the scaling phase, '0' if we don't want consider them in the scaling phase;
    7. 'save_fig' is a integer flag to set to '1' if we want save the pair plots, '0' if we don't want save them.

    In output, if we set 'save_fig' to 0 we will have these folders:

    * *csv_hdbscan*: contain csv files for each 'min_cluster_size' in the cluster process, in these file there is an order in according to the cluster id;
    * *csv_hdbscan_not_sorted_by_cluster*: contain csv files in which the domains are not sorted by cluster id value;
    * *img_sse_avgsil_db*: contain the image that represents the trend curves of SSEs, mean value of Silhouette Scores and Davies-Bloudin indices as the value of k varies.
    * *list_score*: contain a three different '.txt' files, each one contain the value of respectly SSE, mean value of Silhouette Scores and Davies-Bloudin indices.

    The case with 'save_fig' set to 1 is not implemented.

## PCA
* *pca.py*: this is the script to apply the PCA with a predefined number of principal components. It accepts:

    1. the csv path fo the feature;
    2. the number of pc;
    3. the scaler to use ('1' for the StandardScaler,'2' for the MinMax Scaler, other value we apply the clustering without scaling data);
    4. the name of csv file that we will obtain in output;
    5. 'saving' set to 1 we wil obtain the csv with the components specified;
    6. 'top_scale' is a integer flag to set to '1' if we want consider the feature 'A/AAA_top_country' in the scaling phase, '0' if we don't want consider them in the scaling phase;

## anal_clusters

* *conc_vt_inv_score.py*: this is the script to concatenate the given csv by scanning with VirusTotal and UmbrellaInvestigate and the given csv by scanning with Investigate to derive the 'risk_score', moreover this script makes the labeling step. There is a flag (first argument to pass) 'choose' to choose the different function:

    1. 'choose' == 1: you have to pass the 'path of csv resulting from scanning with VT' and 'path of csv resulting from scanning with Investigate to retrieve the risk_score'; the script make a concatenation of this two csv on the common column 'domain';
    2. 'choose' == 2: you have to pass the 'path of csv resulting from scanning with Investigate' and 'path of csv resulting from scanning with Investigate to retrieve the risk_score'; the script make a concatenation of this two csv on the common column 'domain';
    3. 'choose' == 3: you have to pass the 'path of csv resulting from scanning with VT','path of csv resulting from scanning with Investigate' and 'path of csv resulting from scanning with Investigate to retrieve the risk_score'; the script make a concatenation of this two csv on the common column 'domain';
    4. 'choose' == 4: you have to pass the path of the csv obtained by setting 'choose'=1 and a 'limit' value that represent the lowest value of 'positives' (field retrieved by scanning with VT thaw it tells us the number of scanner which  marks as 'bad' a domain name) to consider a domain name as malicious domain name; in this phase we obtained a csv with a 'domain' column and a 'label' column;
    5. 'choose' == 5: you have to pass the path of the csv obtained by setting 'choose'=2; in this phase we obtained a csv with a 'domain' column and a 'label' column.

* *anal_cluster_vt_inv_riskscore.py*: this script is useful to retrieve information about clustering process (graph of perfomance, SIL DB SSE) and information about labeling process (how clusters are formed and by which domains). It accepts:

    1. the path of the folder that contains the csv obtained after the clustering process;
    2. the csv that contain the label per domain names (obtained from conc_vt_inv_score.py with 'choose' 4/5);
    3. a integer value that serve to set up the x axis of the images ('1' the axis x represent the value of 'k' for the KMEANS algorithm, '2' the axis x represent the 'min_c_size' value for the HDBSCAN algorithm, other value for the DBSCAN algoritm).

* *category_for_cluster.py*: this script serves to retrieve the cluster category from 'content_categories'. It accepts:
    
    1. The csv retrieved from cluster process (the csv formed by the feature column and the 'cluster' column);
    2. the csv retrieved by scanning with Umbrella Investigate.

* *most_relevant_feat_sml.py*: this script exploit a RandomForest Classifier to retrieve the most relevant feature to the clustering process. It accepts:

    1. the path of csv obtained from the clustering process (so it must have the 'cluster' column) and for which we want know the most relevant feature which have influenced the clustering process.

* *var_influence.py*: 

# Data Set Preparation Folder

## scripts_avro

* *starter.py*: this is the entry point to download the avro file from OpenIntel repository, retrieve information for the list of domain name passed. It accepts:

    1. 'start_date': starting date -> format 'XXXXYYZZ' -> XXXX=year, YY=month,ZZ=day
    2. 'end_date': end date -> format 'XXXXYYZZ' -> XXXX=year, YY=month,ZZ=day (For this basic usage start date must be equal to start date to download a zip file for one day);
    3. type_data (str): 'umbrella1m', 'open-tld','alexa1m'
    (For example: if you want download tar of alexa1m from 01-01-2020 to 03-01-2020, you have to write:
        - start_date as '20200101'
        - end_date as '20200103'
        - type_data as 'alexa1m'
    )
    4. the path of csv that contain a list of domain names which we want retrieve infomation;
    5. a name folder where put the list of the name that are common and not beetwen the list passed and the domain names queried by OI for a specific day;
    6. 'json_set' a int flag, set '1' to save the infomation retrieved by OI in a json file. 

The *starter.py* calls the following scripts in this order:

* *avro_to_csv.py*: this script has the task to download the zip file (for a specific day and for a specific type) from OpenIntel, unzip it and convert the avro file in csv file;
* *opint_csv.py*: this scrit has the task to retrive the list of domain names queried by OpenIntel and retrieve the intersection between the queried domain and the list of domain names passed;
* *polito_OI.py*: this script has the task to extract the tuple from OpenIntel only fer the domains present in the intersection. 
* *normalization_csvOI.py*: this script has the task to convert the information from the 'tuple' format in a csv in a compact format. A basic example, suppose we have this two tuple:


        query_name        response_name   query_type    ipv4      ttl    rtt
        domainA.com       domainA.com          A        1.0.0.1   0.1     20
        domainA.com       domainA.com          A        1.0.0.2   0.3     13

After the normalization we have:

        domain          A_ipv4_0     A_ttl_0    A_rtt_0   A_ipv4_1    A_ttl_1  A_rtt_1
        domainA.com     1.0.0.1       0.1        20       1.0.0.2      0.3       13
        
Moreover this script extraxt a json file where put the information for domain. For example:

            'domainA.com':{
                'ipv4':[1.0.0.1,1.0.0.2],
                'ttl':[0.1,0.3],
                'rtt':[20,30]
            }


## scripts_umbrella_investigate

* *retrieve_score_investigate.py*: this is the script to scan a list of domain names and retrieve the relative 'risk_score' given by Umbrella. It accepts in input:

    1. the Umbrella Investigate API token:
    2. the csv of domain names to scan.

* *umbrella_investigate_poli_requests.py*: this is the script to scan a list of domain names with Umbrella Investigate. It accepts in input:

    1. the Umbrella Investigate API token;
    2. the csv of domain names to scan.


## scripts_vt

* *20kpolito_vt.py* : this is the script to scan a list of domain names (2nd level domain) with VirusTotal and with a academic key. It accepts in input:

    1. the VirusTotal API token;
    2. the csv of domain names to scan.

## merge_csv

* *merge_csv.py*: this script join on the common column 'domain' the csv obtained from the scanned process with VT and Investigate. It accepts in input:

    1. a folder that contains the csv to merge;
    2. the name to give to the csv in output.

## feature_extraction

* *feature_extraction.py*: this script has the task to convert the infromation in json file obtained from *normalization.py* script in a csv file that will be the our feature set. Moreover the script create the columns 'n_digits','n_characters' and 'n_letter'. It accepts in input:

    1. the path of json file;
    2. the csv contain the list of domain names. 
    3. the name of file out.