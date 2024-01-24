# Media Recommendation System

## Overview
This project implements a media recommendation system using Spring Boot and MongoDB. 
It consists of three main controllers - `HistoryController`, `ItemsController`, and `RegistarationController` - each responsible for specific functionalities related to user history, 
media items, and user registration, respectively.

## some implementation

### 1. Integration with Oracle Database
- Integrated the system with an Oracle database to fetch media items.
 The `ItemsController` includes functionality to copy items from Oracle tables to the MongoDB system storage, ensuring seamless data transfer between different data sources.

### 2. Efficient User Registration and Authentication
- Designed a robust user registration and authentication system using the `RegistarationController`.
 This system efficiently handles new user registrations, checks for existing usernames, and validates user credentials during the login process.
