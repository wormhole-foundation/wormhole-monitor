import { Checkbox, Dialog, DialogContent, DialogTitle, FormControlLabel } from '@mui/material';
import { createContext, useCallback, useContext, useState } from 'react';

type Settings = {
  showDetails: boolean;
  open(): void;
};
const initialSettings: Settings = {
  showDetails: false,
  open: () => {},
};
const SettingsContext = createContext<Settings>(initialSettings);

export function SettingsProvider({ children }: { children: JSX.Element }) {
  const [isOpen, setOpen] = useState<boolean>(false);
  const [value, setValue] = useState<Settings>({
    ...initialSettings,
    open: () => {
      setOpen(true);
    },
  });
  const handleClose = useCallback(() => {
    setOpen(false);
  }, []);
  const handleShowDetailsChange = useCallback((_: any, checked: boolean) => {
    setValue((value) => ({ ...value, showDetails: checked }));
  }, []);

  return (
    <SettingsContext.Provider value={value}>
      {children}
      <Dialog open={isOpen} onClose={handleClose}>
        <DialogTitle>Settings</DialogTitle>
        <DialogContent>
          <FormControlLabel
            control={<Checkbox checked={value.showDetails} onChange={handleShowDetailsChange} />}
            label="Show Details (Experimental)"
          />
        </DialogContent>
      </Dialog>
    </SettingsContext.Provider>
  );
}

export const useSettings = () => useContext(SettingsContext);
