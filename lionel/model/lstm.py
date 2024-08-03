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


class LSTMWithELU(LSTM):
    """
    A Long Short-Term Memory (LSTM) model with Exponential Linear Unit (ELU) activation function.
    Inherits from the base LSTM class.

    Args:
        **kwargs: Additional keyword arguments to be passed to the base LSTM class.

    Attributes:
        elu (nn.ELU): ELU activation function.

    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.elu = nn.ELU()

    def forward(self, x):
        """
        Forward pass of the LSTMWithELU model.

        Args:
            x (torch.Tensor): Input tensor.

        Returns:
            torch.Tensor: Output tensor after passing through the LSTM and ELU layers.

        """
        output = super().forward(x)
        return self.elu(output)


class LSTMWithSELU(LSTM):
    """
    A Long Short-Term Memory (LSTM) model with Scaled Exponential Linear Unit (SELU) activation function.
    Inherits from the base LSTM class.

    Args:
        **kwargs: Additional keyword arguments to be passed to the base LSTM class.

    Attributes:
        selu (nn.SELU): SELU activation function.

    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.selu = nn.SELU()

    def forward(self, x):
        """
        Forward pass of the LSTMWithSELU model.

        Args:
            x (torch.Tensor): Input tensor.

        Returns:
            torch.Tensor: Output tensor after passing through the LSTM and SELU layers.

        """
        output = super().forward(x)
        return self.selu(output)
