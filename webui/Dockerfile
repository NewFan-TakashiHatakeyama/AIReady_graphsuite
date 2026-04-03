FROM node:20-slim

# Set working directory
WORKDIR /app

# Copy package.json and package-lock.json (if available)
COPY package*.json ./

# Install dependencies using npm
RUN npm install

# Copy the rest of the application code
COPY . .

# Fix permissions for node_modules/.bin directory
RUN chmod -R +x node_modules/.bin/

# Expose the port the app runs on
EXPOSE 5173

# Command to run the app with permission fix
CMD ["sh", "-c", "chmod -R +x node_modules/.bin/ && npm run dev"]
