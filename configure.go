package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path"
)

// Config stores credentials and other configurable variables.
type Config struct {
	APIKey string `json:"apikey"`
}

func getHomeDirectory() (string, error) {
	envHome := os.Getenv("HOME")
	if envHome == "" {
		// Fallback to get the home directory from system
		usr, serr := user.Current()
		if serr != nil {
			return "", fmt.Errorf("getHomeDirectory: %v", serr)
		}
		return usr.HomeDir, nil
	}
	return envHome, nil
}

func maskAPIKey(apikey string) string {
	if len(apikey) > 10 {
		return apikey[:7] + "..."
	}
	return apikey
}

func subCmdConfigure() error {
	// Get the home directory of the current user and setup paths
	homeDir, serr := getHomeDirectory()
	if serr != nil {
		return fmt.Errorf("configure: %v", serr)
	}
	configDirPath := path.Join(homeDir, configDirectoryName)
	configFilePath := path.Join(configDirPath, configFileName)

	var config Config
	// Load the config file if exist
	if _, serr := os.Stat(configFilePath); !os.IsNotExist(serr) {
		data, serr := ioutil.ReadFile(configFilePath)
		if serr != nil {
			return fmt.Errorf("configure: load: %v", serr)
		}
		serr = json.Unmarshal(data, &config)
		if serr != nil {
			return fmt.Errorf("configure: load: %v", serr)
		}
	}

	fmt.Println("Enter your Exchangedataset credentials")
	fmt.Println("^C to cancel")
	fmt.Printf("API-key[%s]: ", maskAPIKey(config.APIKey))
	fmt.Scanln(&config.APIKey)

	fmt.Printf("Writing to %s\n", configFilePath)
	// Marshal entered config
	marshaled, serr := json.Marshal(config)
	if serr != nil {
		return fmt.Errorf("configure: %v", serr)
	}
	if stat, serr := os.Stat(configDirPath); os.IsNotExist(serr) {
		// Make a directory if it does not exist
		serr = os.Mkdir(configDirPath, 0755)
		if serr != nil {
			return fmt.Errorf("configure: %v", serr)
		}
	} else {
		if serr != nil {
			return fmt.Errorf("configure: %v", serr)
		}
		if !stat.IsDir() {
			return fmt.Errorf("configure: %s is not a directory, please remove it", configDirPath)
		}
	}
	// Create (if not exists) and write to the file
	serr = ioutil.WriteFile(configFilePath, marshaled, 0700)
	if serr != nil {
		return fmt.Errorf("configure: %v", serr)
	}
	fmt.Println("Config updated")
	return nil
}
