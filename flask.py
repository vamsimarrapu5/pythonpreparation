from flask import Flask, render_template, request
import logging

app = Flask(__name__)

# Set up logging
logging.basicConfig(filename='error.log', level=logging.ERROR)

# Custom exception class
class CustomException(Exception):
    pass

# Global error handler for all exceptions
@app.errorhandler(Exception)
def handle_exception(error):
    app.logger.error('An error occurred: %s', error)
    return render_template('error.html', error_message='An unexpected error occurred'), 500

# Custom error handler for 404 Not Found errors
@app.errorhandler(404)
def page_not_found(error):
    app.logger.error('404 Error: Page not found: %s', request.path)
    return render_template('error.html', error_message='Page Not Found'), 404

# Custom error handler for 500 Internal Server Error
@app.errorhandler(500)
def internal_server_error(error):
    app.logger.error('500 Error: Internal server error: %s', error)
    return render_template('error.html', error_message='Internal Server Error'), 500

# Route to raise a custom exception
@app.route('/custom_error')
def custom_error():
    raise CustomException("This is a custom exception.")

@app.route('/')
def index():
    return 'Hello, World! This is the homepage.'

@app.route('/about')
def about():
    return 'This is the about page.'

@app.route('/user/<username>')
def user_profile(username):
    return f'Hello, {username}! Welcome to your profile.'

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        # Add your login logic here, like checking credentials
        return f'Logged in as {username}'
    else:
        return render_template('login.html')

if __name__ == '__main__':
    app.run(debug=True)
