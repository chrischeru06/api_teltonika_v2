const express = require('express');
const bodyParser = require('body-parser');
const ping = require('ping');
const https = require('https')
const http = require('http')
const fs = require('fs');
const path = require("path");
const app = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use((req, res, next) => {
   res.setHeader('Access-Control-Allow-Origin', '*');
   res.setHeader('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content, Accept, Content-Type, Authorization');
   res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, PATCH, OPTIONS');
   next();
 });

var smpp = require('smpp');


app.get("/", function (request, response) {
  response.status(200).send("cgm smpp server");
});


app.get("/ping", function (request, response) {
  
		var hosts = ['10.213.213.24'];
		var message = "";
		
		hosts.forEach(function(host){
		    ping.sys.probe(host, function(isAlive){
		        var msg = isAlive ? 'host ' + host + ' is alive' : 'host ' + host + ' is dead';
		        console.log(msg);
		        message += msg
		        response.status(200).send(msg);
		       
		    });

		     
		});

});



app.post("/sms_pafe", function (request, response) {
   var telephone = request.body.phone;
   var txt_message = request.body.txt_message;
   console.log(request.body)
   
 

   var session = smpp.connect({
	url: 'smpp://10.213.213.24:5016',
	auto_enquire_link_period: 1000,
	debug: true
	}, function() {
		session.bind_transceiver({
			system_id: 'CGM',
			password: 'cgm23'
		}, function(pdu) {
			if (pdu.command_status === 0) {
				// Successfully bound
				session.submit_sm({
					destination_addr: telephone,
					short_message: txt_message
				}, function(pdu) {
					
					if (pdu.command_status === 0) {
						// Message successfully sent
					  console.log(pdu.message_id);
						response.status(200).send(pdu.message_id);
						
					}
				});
			}
		});
	});
});


app.post("/sms", function (request, response) {
          var telephone = request.body.phone;
          var txt_message = request.body.txt_message;
          
          //var telephone = "79802611";
          //var txt_message = "Bonjour, Mr Thaddee ce message est un message envoyee dans le contexte de test d'envoi de SMS par protocol SMPP. Merci";
       
         /*New => 10.213.213.24 port 5016.
           Old => 10.230.230.11:5016
         */
          var session = smpp.connect({
                 url: 'smpp://10.213.213.24:5016',
                 auto_enquire_link_period: 1000,
                 debug: true
                 }, function() {
                           session.bind_transceiver({
                                     system_id: 'MEDIABOX',
                                     password: 'medi!@22'
                           }, function(pdu) {
                                     if (pdu.command_status === 0) {
                                               // Successfully bound
                                               session.submit_sm({
                                                         destination_addr: telephone,
                                                         short_message: txt_message
                                               }, function(pdu) {
                                                         
                                                         if (pdu.command_status === 0) {
                                                                   // Message successfully sent
                                                           console.log(pdu.message_id);
                                                                   response.status(200).send(pdu.message_id);
                                                                   
                                                         }
                                               });
                                     }
                           });
                 });
       });

const isHttps = true
var server
if (isHttps) {
          var options = {
                    key: fs.readFileSync('/var/www/html/api/https/privkey.pem'),
                    cert: fs.readFileSync('/var/www/html/api/https/fullchain.pem')
          };
          server = https.createServer(options, app)
} else {
          server = http.createServer(app);
}
const port = 22629;

server.listen(port, async () => {
  console.log("server is running on port: " + port);
});


