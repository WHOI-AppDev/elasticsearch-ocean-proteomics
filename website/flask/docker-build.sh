# http://containertutorials.com/docker-compose/flask-simple-app.html
#
# Requires:
# docker (1.6.0 or above)
# python 2.7 or above
# Linux VM - (We used ubuntu 14.04 64 bit)
#
docker build -t ocean-proteins:latest .
docker run -d -p 5000:5000 ocean-proteins
docker ps -a
