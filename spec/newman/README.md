## Testing Bendo Deployments with Newman

### Variables Needed
Variable Name | Variable Value
------------- | ---------------
BendoURL | The URL to test against. *ex*: **bendo.example.com**
BendoPort | The port to test against *ex*: **80**


### Testing Locally

The easiest way to run these tests is to leverage the official [Newman Docker Image](https://hub.docker.com/r/postman/newman) from Postman Labs.

Follow these steps:

Pull down the Docker container for Newman (** Note: this is only needed prior to the first run on local machine**):
 ``` console
docker pull postman/newman
```

Clone the Bendo Repository ( ** Note: this is only required if you do not have a copy of the repository on your machine. **):
 ``` console
git clone git@github.com:ndlib/bendo.git
```

Run the Newman collection against the desired RabbitMQ server:

 ``` console
docker run -v /full/path/to/bendo/spec/newman:/etc/newman -t postman/newman run Bendo_newman_collection.json --global-var "BendoURL=value" --global-var "BendoPort=value"
```
