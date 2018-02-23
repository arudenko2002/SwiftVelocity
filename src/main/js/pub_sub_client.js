// Imports the Google Cloud client library
  const PubSub = require('@google-cloud/pubsub');
  const nodemailer = require('nodemailer');

  var transporter = nodemailer.createTransport({
    host: 'usaws17wviis002.global.umusic.net',
    port: 25,
    secure: false
  });

  // Your Google Cloud Platform project ID
  const projectId = 'umg-swift';

  // Instantiates a client
  const pubsubClient = PubSub({
    projectId: projectId
  });

  var timeout=process.argv[2];
  var print_email = process.argv[3];

  var subscriptionName = 'projects/umg-swift/subscriptions/velocity_alerts';
  console.log(subscriptionName);
  //const timeout = 300;


  const subscription = pubsubClient.subscription(subscriptionName);

  // Create an event handler to handle messages
  let messageCount = 0;
  const messageHandler = message => {
    console.log(`Received message ${message.id}:`);
    console.log(`\tData: ${message.data.toString()}`);
    //console.log(`\tAttributes: ${message.attributes}`);

    for(var key in message.attributes) {
        if(message.attributes.hasOwnProperty(key)) {
            console.log(`\tAttributes: ${key}`);
            var email = message.attributes[key];
            console.log(`\tAttributes: ${email}`);
        }
    }
    var data = message.data.toString();
    var email = message.attributes["email"];
    var firstname = message.attributes["firstname"];
    var lastname = message.attributes["lastname"];
    var subject = message.attributes["subject"]+" for "+firstname+" "+lastname;


    var mailOptions = {
        from: 'noreply@umusic.com',
        to: email,
        subject: subject,
        html: '<b>'+data+'</b>'
    };

    if(print_email == "email") {
        transporter.sendMail(mailOptions, function(error, info){
            if (error) {
                console.log(error);
            } else {
                console.log('Email sent: ' + info.response);
            }
        });
    }
    messageCount += 1;

    // "Ack" (acknowledge receipt of) the message
    message.ack();
  };

  // Listen for new messages until timeout is hit
  subscription.on(`message`, messageHandler);
  setTimeout(() => {
    subscription.removeListener('message', messageHandler);
    console.log(`${messageCount} message(s) received.`);
  }, timeout * 1000);


