package com.me.drools_poc;

import com.google.gson.Gson;

public class GsonWrapper {
	private static Gson gson = new Gson();

	public static Gson gson() {
		return gson;
	}
}
