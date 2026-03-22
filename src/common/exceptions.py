class AppError(Exception):
  """Base application error."""
  status_code: int = 500

  def __init__(self, message: str = "Internal server error"):
    self.message = message
    super().__init__(self.message)


class ValidationError(AppError):
  status_code = 400

  def __init__(self, message: str = "Validation error"):
    super().__init__(message)


class AuthenticationError(AppError):
  status_code = 403

  def __init__(self, message: str = "Authentication failed"):
    super().__init__(message)


class ExternalServiceError(AppError):
  status_code = 502

  def __init__(self, message: str = "External service error"):
    super().__init__(message)
