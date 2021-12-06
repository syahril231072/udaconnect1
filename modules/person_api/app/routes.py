def register_routes(api, app, root="api"):
    from app.udaconnect import register_routes as attach_person

    # Add routes
    attach_person(api, app)
