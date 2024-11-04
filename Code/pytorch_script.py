import pandas as pd
import torch
from torch.utils.data import Dataset, DataLoader

file_path = 'data/Pockets_snapshot.xlsx'
df = pd.read_excel(file_path)

# Step 2: Define a custom dataset class
class PocketsDataset(Dataset):
    def __init__(self, dataframe):
        self.dataframe = dataframe
        self.features = dataframe.iloc[:, 4:].values  # Columns starting from Tooth 18 B
        self.labels = dataframe['CHART TITLE'].values  # Assume CHART TITLE as labels for simplicity

    def __len__(self):
        return len(self.dataframe)

    def __getitem__(self, idx):
        # Convert features and labels to tensors
        x = torch.tensor(self.features[idx], dtype=torch.float32)
        y = torch.tensor(self.labels[idx], dtype=torch.float32)  # Adjust dtype as necessary
        return x, y

# Step 3: Create an instance of the dataset
dataset = PocketsDataset(df)

# Step 4: Create the DataLoader
dataloader = DataLoader(dataset, batch_size=16, shuffle=True)

# Example of iterating through the DataLoader
for batch in dataloader:
    inputs, labels = batch
    print(inputs)
    print(labels)