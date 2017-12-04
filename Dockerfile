FROM fedora:25
RUN dnf -y upgrade
RUN curl --silent --location https://rpm.nodesource.com/setup_7.x | bash -
RUN dnf -y install nodejs
RUN dnf -y clean all
ADD orion.js elasticsearchIndex.js task.js config.js endpoint.js feeder.js helpers.js index.js log.js package.json /opt/feeder/
ADD config/default.yaml config/custom-environment-variables.yaml /opt/feeder/config/

WORKDIR /opt/feeder
RUN npm install

EXPOSE 9000

CMD ["/usr/bin/node", "index.js"]
