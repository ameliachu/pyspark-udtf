from .image_caption import BatchInferenceImageCaption

__all__ = ["BatchInferenceImageCaption"]

# Conditionally export Arrow version if available
try:
    from .image_caption import ArrowBatchInferenceImageCaption
    __all__.append("ArrowBatchInferenceImageCaption")
except ImportError:
    pass
