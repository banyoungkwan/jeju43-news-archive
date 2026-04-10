FROM nginx:alpine
RUN rm -f /etc/nginx/conf.d/default.conf
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY index.html about.html og-image.jpg robots.txt sitemap.xml /usr/share/nginx/html/
RUN chmod -R 755 /usr/share/nginx/html && \
    chown -R nginx:nginx /usr/share/nginx/html
EXPOSE 8080
CMD ["nginx", "-g", "daemon off;"]
