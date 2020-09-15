package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path"

	"github.com/exchangedataset/exdgo"
)

var config *Config

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

func initConfig() error {
	homeDir, serr := getHomeDirectory()
	if serr != nil {
		return fmt.Errorf("initConfig: %v", serr)
	}
	configPath := path.Join(homeDir, configDirectoryName, configFileName)
	if _, serr := os.Stat(configPath); os.IsNotExist(serr) {
		return fmt.Errorf("initConfig: config has not yet setup. please run '%s configure'", os.Args[0])
	} else if serr != nil {
		return fmt.Errorf("initConfig: %v", serr)
	}
	data, serr := ioutil.ReadFile(configPath)
	if serr != nil {
		return fmt.Errorf("initConfig: %v", serr)
	}
	config = new(Config)
	serr = json.Unmarshal(data, config)
	if serr != nil {
		return fmt.Errorf("initConfig: %v", serr)
	}
	return nil
}

func makeClientParam() exdgo.ClientParam {
	var cp exdgo.ClientParam
	cp.APIKey = config.APIKey
	return cp
}

func maskAPIKey(apikey string) string {
	if len(apikey) > 10 {
		return apikey[:7] + "..."
	}
	return apikey
}

func subCmdConfigure() (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("configure: %v", err)
		}
	}()

	// Get the home directory of the current user and setup paths
	homeDir, err := getHomeDirectory()
	if err != nil {
		return
	}
	configDirPath := path.Join(homeDir, configDirectoryName)
	configFilePath := path.Join(configDirPath, configFileName)

	var config Config
	// Load the config file if exist
	// Ignoring a stat error is intentional
	if _, err := os.Stat(configFilePath); !os.IsNotExist(err) {
		data, err := ioutil.ReadFile(configFilePath)
		if err != nil {
			return fmt.Errorf("load: %v", err)
		}
		err = json.Unmarshal(data, &config)
		if err != nil {
			return fmt.Errorf("load: %v", err)
		}
	}

	_, err = fmt.Println("Enter your Exchangedataset credentials")
	if err != nil {
		return
	}
	_, err = fmt.Println("^C to cancel")
	if err != nil {
		return
	}
	_, err = fmt.Printf("API-key[%s]: ", maskAPIKey(config.APIKey))
	if err != nil {
		return
	}
	_, err = fmt.Scanln(&config.APIKey)
	if err != nil {
		return
	}

	_, err = fmt.Printf("Writing to %s\n", configFilePath)
	if err != nil {
		return
	}
	// Marshal entered config
	marshaled, err := json.Marshal(config)
	if err != nil {
		return
	}
	var stat os.FileInfo
	if stat, err = os.Stat(configDirPath); os.IsNotExist(err) {
		// Make a directory if it does not exist
		err = os.Mkdir(configDirPath, 0755)
		if err != nil {
			return
		}
	} else {
		if err != nil {
			return
		}
		if !stat.IsDir() {
			return fmt.Errorf("%s is not a directory, please remove it", configDirPath)
		}
	}
	// Create (if not exists) and write to the file
	err = ioutil.WriteFile(configFilePath, marshaled, 0700)
	if err != nil {
		return
	}
	_, err = fmt.Println("Config updated")
	return
}
