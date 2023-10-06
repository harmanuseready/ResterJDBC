{
   "ENV":{
      "authenticationType":"oauth",
      "followHTTPRedirect":false,
      "baseUrl":"https://kntcewove0.execute-api.us-east-1.amazonaws.com/dev",
      "levels":"1",
      "format":"oneCall",
      "database":"/dz/getviewsjson?format=json",
      "fields":[
         {
            "redirect":"true",
            "columns" : [ ],
			"kwargs" : { },
			"url" : "/dz/getviewurl?ViewName={}&format=json"

         }
      ]
   },
   "UTR":{
      "authenticationType":"oauth",
      "followHTTPRedirect":false,
      "baseUrl":"https://kntcewove0.execute-api.us-east-1.amazonaws.com/dev",
      "levels":"1",
      "format":"oneCall",
      "database":"/dz/getviewsjson?format=json",
      "fields":[
         {
            "redirect":"true",
           "columns" : [ ],
			"kwargs" : { }, 
			"url" : "/dz/getviewurl?ViewName={}&&format=json"

         }
      ]
   },
   "PROD":{
      "authenticationType":"oauth",
      "followHTTPRedirect":false,
      "baseUrl":"https://kntcewove0.execute-api.us-east-1.amazonaws.com/dev",
      "levels":"1",
      "format":"oneCall",
      "database":"/dz/getviewsjson?format=json",
      "fields":[
         {
            "redirect":"true",
            "columns" : [ ],
			"kwargs" : { },
			"url" : "/dz/getviewurl?ViewName={}&&format=json"

         }
      ]
   }
}