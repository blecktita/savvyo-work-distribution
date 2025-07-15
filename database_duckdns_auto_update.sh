#!/bin/bash

# DuckDNS Update Script for Machine A
# Save this as: /home/user/duckdns/duck.sh

# Your DuckDNS configuration
SUBDOMAIN="savvyo"
TOKEN="ac7179dc-2ea8-4779-b331-e602bd13c8f2"

# Update DuckDNS with current IP
echo url="https://www.duckdns.org/update?domains=$SUBDOMAIN&token=$TOKEN&ip=" | curl -k -o /home/user/duckdns/duck.log -K -

# Log the update
echo "$(date): DuckDNS updated" >> /home/user/duckdns/duck.log