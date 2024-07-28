import torch
import torch.nn as nn
from neuralforecast.models import LSTM


class LSTMWithReLU(LSTM):
    """
    A Long Short-Term Memory (LSTM) model with Rectified Linear Unit (ReLU) activation function.
    Inherits from the base LSTM class.

    Args:
        **kwargs: Additional keyword arguments to be passed to the base LSTM class.

    Attributes:
        relu (nn.ReLU): ReLU activation function.

    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.relu = nn.ReLU()

    def forward(self, x):
        """
        Forward pass of the LSTMWithReLU model.

        Args:
            x (torch.Tensor): Input tensor.

        Returns:
            torch.Tensor: Output tensor after passing through the LSTM and ReLU layers.

        """
        output = super().forward(x)
        return self.relu(output)
