# Prometheus
[Source code](https://github.com/prometheus/prometheus)

## Customized Changes

### 1. Support api for operate scrape job

* Common

    ```
    Header: Content-type:application/json
    Success Code: 200
    Failed Code: 500    
    ```    
 
* Get current jobs: `GET /scrape_list`
    
    Response:
    ```
      Success:    
      {
      	"extra": ["blackbox_exporter2"],//the job added by api
      	"original": ["http_2xx", "node_Exporter"]//the job added by config
      }
      
      Failed: error msg
    ```

* Add a new scrape job: `POST /scrape_job`

    Post Data:
    ```
    {
        "job_name": "bloack_box_test2",//must unique
        "honor_timestamps": true,//use default
        "metrics_path": "/probe",//use default
        "scheme": "http",//use default
        "params": {//the param of job
            "module": ["http_2xx"]
        },
        "scrape_interval": "3s",
        "targets": ["http://www.99cloud.com", "http://utf9.me.com", "https://nga.178.com"],//scrape targets
        "relabel_configs": [
            {
                "source_labels": ["__address__"],
                "target_label": "__param_target",
                "regexp_str": "avcd!@#$^"//default:(.*),not required
            },
            {
            	"source_labels": ["__param_target"],
            	"target_label": "instance",
            },
            {
            	"target_label": "__address__",
            	"replacement": "127.0.0.1:9115",
            }
        ]
    }
    
    Failed: error msg
    ```
    Response:
    ```
      Success:  
      //New tasks will appear in the list
      {
        "extra": ["blackbox_exporter2"],//the job added by api
        "original": ["http_2xx", "node_Exporter"]//the job added by config
      }
      
      Failed: error msg
    
    ```
    
* Delete job by name: ` /scrape_job/{jobName}`

    Response:
    ```
    Success: ""
    
    Failed: error msg
    
    ```

### 2. Add rule dynamically


* Add new rule group: `POST /rule`

    Post Data:
    ```
    {
    	"name": "test_group",
    	"interval": "5s",
    	"rules": [
    		{
    			"record": "probeFailed",
    			"expr": "probe_success < 1",
    			"for": "3s",
    			"labels": {
    				"a": "aa",
    				"b": "bb"
    			},
    			"annotations": {
    				"c": "cc",
    				"d": "dd"
    			}
    		},
    		{
    			"record": "probeFailed1",
    			"expr": "probe_success < 1",
    			"for": "3s",
    			"labels": {
    				"a": "aa",
    				"b": "bb"
    			},
    			"annotations": {
    				"c": "cc",
    				"d": "dd"
    			}
    		}
    	]
    }
    
    Failed: error msg
    ```
    Response:
    ```
      Success:  
      //New tasks will appear in the list
      {
        "extra": ["blackbox_exporter2"],//the job added by api
        "original": ["http_2xx", "node_Exporter"]//the job added by config
      }
      
      Failed: error msg
    
    ```
    
### 3. Insert custom metric


* Add new rule group: `POST /insert_metric`

    Post Data:
    ```
    {
    	"metric_name": "test_metric_name",
    	"val": 1, //float64
    	"labels": [
    		{
    			"label_name": "job",
    			"label_value": "test_job"
    		},
    		{
    	   		"label_name": "instance",
            	"label_value": "127.0.0.1"
    		}
    	]
    }
    
    Failed: error msg
    ```
    Response:
    ```
      Success:  
      Failed: error msg
    
    ```    
